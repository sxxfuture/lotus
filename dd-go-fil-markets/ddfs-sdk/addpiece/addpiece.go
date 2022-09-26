//DD add
package addpiece

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"os"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/detailyang/go-fallocate"
	rlepluslazy "github.com/filecoin-project/go-bitfield/rle"
	"github.com/filecoin-project/go-fil-markets/ddfs-sdk/api"
	"github.com/filecoin-project/go-fil-markets/ddfs-sdk/proxyreader"
	"github.com/filecoin-project/go-fil-markets/ddfs-sdk/utils"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/net/context"
)

var log = logging.Logger("ddfs-addpiece")

func GenUnseal(ctx context.Context, unsealPath string, sector abi.SectorID, pieceSize abi.UnpaddedPieceSize) (abi.PieceInfo, error) {
	var err error
	var pieceCid cid.Cid
	_pieceCid, ok := ctx.Value("pieceCID").(string)
	if !ok {
		return abi.PieceInfo{}, nil
	}

	pieceCid, err = cid.Decode(_pieceCid)
	if err != nil {
		log.Errorf("[DD] Decode PieceCid sector: %v", sector)
		return abi.PieceInfo{}, err
	}

	remoteFileUrl, _ := ctx.Value("remoteFileUrl").(string)

	var confirm func(string) error
	var file io.ReadCloser

	file, confirm, err = GetRemoteFile(remoteFileUrl)
	if err != nil {
		log.Errorf("[DD] get remote car file err: %v", err)
		return abi.PieceInfo{}, err
	}
	defer file.Close()

	defer func() {
		if err == nil && confirm != nil {
			ConfirmProxy(confirm, remoteFileUrl, sector, 60, time.Minute*2)
		}
	}()

	maxPieceSize := abi.PaddedPieceSize(pieceSize.Padded())

	beginAddPieceT := time.Now()
	log.Infof("[DD] before generate unseal to %v", unsealPath)

	err = genUnsealDDOptimize(file, unsealPath, maxPieceSize)
	if err != nil {
		log.Errorf("[DD] generate unseal failed err: %v", err)
		return abi.PieceInfo{}, err
	}

	log.Infof("[DD] after generate unseal to %v,cost:[%v]", unsealPath, time.Since(beginAddPieceT).Truncate(time.Second))
	return abi.PieceInfo{
		pieceSize.Padded(),
		pieceCid,
	}, nil
}

func GetRemoteFile(remoteFileUrl string) (api.RemoteFile, func(string) error, error) {
	fOpt, err := api.NewFileOpt(remoteFileUrl)
	if err != nil {
		return nil, nil, err
	}
	proxyReader, err := proxyreader.NewReaderProxy(fOpt.Fetch, 60, time.Minute)
	if err != nil {
		return nil, nil, err
	}
	return proxyReader, fOpt.Confirm, nil
}

func ConfirmFetched(remoteFileUrl string, id abi.SectorID) error {
	fileOpt, err := api.NewFileOpt(remoteFileUrl)
	if err != nil {
		return err
	}
	key := utils.SectorName(id)
	err = fileOpt.Confirm(key)
	if err != nil {
		log.Errorf("[DD] confirm %v failed,key: %v,err: %v", remoteFileUrl, key, err)
		ConfirmProxy(fileOpt.Confirm, remoteFileUrl, id, 60, time.Minute*2)
		return nil
	}
	log.Infof("[DD] succeed to confirm %v key: %v", remoteFileUrl, key)
	return nil
}

func ConfirmProxy(confirm func(string) error, remoteFileUrl string, id abi.SectorID, errLimit int, errWait time.Duration) {
	key := utils.SectorName(id)
	err := confirm(key)
	if err == nil {
		log.Infof("succeed to confirm %v key: %v", remoteFileUrl, key)
		return
	}
	log.Errorf("confirm %v failed,key: %v,err: %v,errNum: %v", remoteFileUrl, key, err, 0)

	go func() {
		for i := 1; i < errLimit; {
			err := confirm(key)
			if err == nil {
				log.Infof("succeed to confirm %v key: %v", remoteFileUrl, key)
				return
			}
			log.Errorf("confirm %v failed,key: %v,err: %v,errNum: %v", remoteFileUrl, key, err, i)
			i++
			if i < errLimit {
				utils.SleepRandT(errWait)
			}
		}
	}()
}

func genUnsealDDOptimize(car io.Reader, path string, maxPieceSize abi.PaddedPieceSize) error {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644) // nolint
	if err != nil {
		return fmt.Errorf("openning partial file '%s': %w", path, err)
	}

	defer func() {
		_ = f.Close()
		if err != nil {
			_ = os.Remove(path)
		}
	}()

	err = func() error {
		err := fallocate.Fallocate(f, 0, int64(maxPieceSize))
		if errno, ok := err.(syscall.Errno); ok {
			if errno == syscall.EOPNOTSUPP || errno == syscall.ENOSYS {
				log.Warnf("could not allocate space, ignoring: %v", errno)
				err = nil // log and ignore
			}
		}
		if err != nil {
			return fmt.Errorf("fallocate '%s': %w", path, err)
		}

		if err := writeTrailer(int64(maxPieceSize), f, &rlepluslazy.RunSliceIterator{}); err != nil {
			return fmt.Errorf("writing trailer: %w", err)
		}
		return nil
	}()
	if err != nil {
		return err
	}

	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	err = toUnseal(car, f)
	if err != nil {
		return fmt.Errorf("copy car: %w", err)
	}

	ored := PieceRun(0, maxPieceSize)
	err = writeTrailer(int64(maxPieceSize), f, ored)
	if err != nil {
		return err
	}
	return nil
}

const bufLength = (4 << 20) * 127 / 128

func toUnseal(src io.Reader, dst io.Writer) error {
	type dataMeta struct {
		data []byte
		err  error
	}
	dataCh := make(chan dataMeta, 1)
	fr32Ch := make(chan dataMeta, 1)
	closeRead := make(chan struct{})
	go func() {
		for {
			buf := make([]byte, bufLength)
			n, err := io.ReadFull(src, buf)

			select {
			case dataCh <- dataMeta{
				buf[:n],
				err,
			}:
				if err != nil {
					return
				}
			case <-closeRead:
				return
			}
		}
	}()

	go func() {
		for {
			var data dataMeta
			select {
			case data = <-dataCh:
			case <-closeRead:
				return
			}

			if len(data.data) != 0 {
				data.data = genFr32(data.data)
			}

			select {
			case fr32Ch <- data:
				if data.err != nil {
					return
				}
			case <-closeRead:
				return
			}
		}
	}()

	var stop bool
	for {
		data := <-fr32Ch

		if data.err != nil {
			if !errors.Is(data.err, io.ErrUnexpectedEOF) && !errors.Is(data.err, io.EOF) {
				return data.err
			}
			stop = true
		}

		_, err := dst.Write(data.data)
		if err != nil {
			close(closeRead)
			return err
		}

		if stop {
			return nil
		}
	}
}

func writeTrailer(maxPieceSize int64, w *os.File, r rlepluslazy.RunIterator) error {
	trailer, err := rlepluslazy.EncodeRuns(r, nil)
	if err != nil {
		return fmt.Errorf("encoding trailer: %w", err)
	}

	// maxPieceSize == unpadded(sectorSize) == trailer start
	if _, err := w.Seek(maxPieceSize, io.SeekStart); err != nil {
		return fmt.Errorf("seek to trailer start: %w", err)
	}

	rb, err := w.Write(trailer)
	if err != nil {
		return fmt.Errorf("writing trailer data: %w", err)
	}

	if err := binary.Write(w, binary.LittleEndian, uint32(len(trailer))); err != nil {
		return fmt.Errorf("writing trailer length: %w", err)
	}

	return w.Truncate(maxPieceSize + int64(rb) + 4)
}

func genFr32(car []byte) []byte {
	if len(car)%127 != 0 {
		pad := make([]byte, 127-len(car)%127)
		car = append(car, pad...)
	}

	fr32File := _GenFr32(car)
	return fr32File
}

func _GenFr32(p []byte) []byte {
	in := p
	if len(p)%127 != 0 {
		panic("length of car file must multiples of 127")
	}
	biggest := abi.UnpaddedPieceSize(len(p))
	work := make([]byte, biggest.Padded(), biggest.Padded())
	Pad(in[:int(biggest)], work)
	return work
}

var MTTresh = uint64(4 << 20)

func Pad(in, out []byte) {
	// Assumes len(in)%127==0 and len(out)%128==0
	if len(out) > int(MTTresh) {
		mt(in, out, len(out), pad)
		return
	}

	pad(in, out)
}

func mtChunkCount(usz abi.PaddedPieceSize) uint64 {
	threads := (uint64(usz)) / MTTresh
	if threads > uint64(runtime.NumCPU()) {
		threads = 1 << (bits.Len32(uint32(runtime.NumCPU())))
	}
	if threads == 0 {
		return 1
	}
	if threads > 32 {
		return 32 // avoid too large buffers
	}
	return threads
}

func mt(in, out []byte, padLen int, op func(unpadded, padded []byte)) {
	padCount := padLen / 128
	threads := mtChunkCount(abi.PaddedPieceSize(padLen))
	threadPadCount := padCount / int(threads)
	off := padCount % int(threads)
	var wg sync.WaitGroup
	wg.Add(int(threads))

	for i := 0; i < int(threads); i++ {
		go func(thread int) {
			defer wg.Done()
			var start abi.PaddedPieceSize
			var end abi.PaddedPieceSize
			if thread < off {
				start = abi.PaddedPieceSize(threadPadCount*thread+thread) * 128
				end = start + abi.PaddedPieceSize(threadPadCount+thread+1)*128
			} else {
				start = abi.PaddedPieceSize(threadPadCount*thread+off) * 128
				end = abi.PaddedPieceSize(threadPadCount*thread+threadPadCount+off) * 128
			}

			op(in[start.Unpadded():end.Unpadded()], out[start:end])
		}(i)
	}
	wg.Wait()
}

func pad(in, out []byte) {
	chunks := len(out) / 128
	for chunk := 0; chunk < chunks; chunk++ {
		inOff := chunk * 127
		outOff := chunk * 128

		copy(out[outOff:outOff+31], in[inOff:inOff+31])

		t := in[inOff+31] >> 6
		out[outOff+31] = in[inOff+31] & 0x3f
		var v byte

		for i := 32; i < 64; i++ {
			v = in[inOff+i]
			out[outOff+i] = (v << 2) | t
			t = v >> 6
		}

		t = v >> 4
		out[outOff+63] &= 0x3f

		for i := 64; i < 96; i++ {
			v = in[inOff+i]
			out[outOff+i] = (v << 4) | t
			t = v >> 4
		}

		t = v >> 2
		out[outOff+95] &= 0x3f

		for i := 96; i < 127; i++ {
			v = in[inOff+i]
			out[outOff+i] = (v << 6) | t
			t = v >> 2
		}

		out[outOff+127] = t & 0x3f
	}
}
