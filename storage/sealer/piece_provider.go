package sealer

import (
	"bufio"
	"context"
	"io"

	"os"
	"fmt"
	"net"
	"time"
	"path/filepath"
	"io/ioutil"
	"encoding/json"
	"strings"
	"os/exec"

	gossh "golang.org/x/crypto/ssh"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"

	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/lotus/storage/paths"
	"github.com/filecoin-project/lotus/storage/sealer/fr32"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
)

type Unsealer interface {
	// SectorsUnsealPiece will Unseal a Sealed sector file for the given sector.
	SectorsUnsealPiece(ctx context.Context, sector storiface.SectorRef, offset storiface.UnpaddedByteIndex, size abi.UnpaddedPieceSize, randomness abi.SealRandomness, commd *cid.Cid) error
}

type SshManager struct {
	/** ip */
	Host string `json:"host"`
	/** 端口 */
	Port int `json:"port"`
	/** 用户 */
	Username string `json:"user"`
	/** 密码 */
	Password string `json:"password"`
	/**  */
	client *gossh.Client
}

// add by lin
type CarDir struct {
	Miner string
	Dir []string
}
// end

type PieceProvider interface {
	// ReadPiece is used to read an Unsealed piece at the given offset and of the given size from a Sector
	// pieceOffset + pieceSize specify piece bounds for unsealing (note: with SDR the entire sector will be unsealed by
	//  default in most cases, but this might matter with future PoRep)
	// startOffset is added to the pieceOffset to get the starting reader offset.
	// The number of bytes that can be read is pieceSize-startOffset
	ReadPiece(ctx context.Context, sector storiface.SectorRef, pieceOffset storiface.UnpaddedByteIndex, pieceSize abi.UnpaddedPieceSize, ticket abi.SealRandomness, unsealed cid.Cid) (mount.Reader, bool, error)
	IsUnsealed(ctx context.Context, sector storiface.SectorRef, offset storiface.UnpaddedByteIndex, size abi.UnpaddedPieceSize) (bool, error)
}

var _ PieceProvider = &pieceProvider{}

type pieceProvider struct {
	storage *paths.Remote
	index   paths.SectorIndex
	uns     Unsealer
}

func NewPieceProvider(storage *paths.Remote, index paths.SectorIndex, uns Unsealer) PieceProvider {
	return &pieceProvider{
		storage: storage,
		index:   index,
		uns:     uns,
	}
}

// IsUnsealed checks if we have the unsealed piece at the given offset in an already
// existing unsealed file either locally or on any of the workers.
func (p *pieceProvider) IsUnsealed(ctx context.Context, sector storiface.SectorRef, offset storiface.UnpaddedByteIndex, size abi.UnpaddedPieceSize) (bool, error) {
	if err := offset.Valid(); err != nil {
		return false, xerrors.Errorf("offset is not valid: %w", err)
	}
	if err := size.Validate(); err != nil {
		return false, xerrors.Errorf("size is not a valid piece size: %w", err)
	}

	ctxLock, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := p.index.StorageLock(ctxLock, sector.ID, storiface.FTUnsealed, storiface.FTNone); err != nil {
		return false, xerrors.Errorf("acquiring read sector lock: %w", err)
	}

	return p.storage.CheckIsUnsealed(ctxLock, sector, abi.PaddedPieceSize(offset.Padded()), size.Padded())
}

func (p *pieceProvider) tryReadUnsealedPieceOfSxx(ctx context.Context, pc cid.Cid, sector storiface.SectorRef, pieceOffset storiface.UnpaddedByteIndex, size abi.UnpaddedPieceSize) (mount.Reader, error) {
	// acquire a lock purely for reading unsealed sectors
	log.Errorf("zlin: tryReadUnsealedPiece")
	ctx, cancel := context.WithCancel(ctx)

	// 优先从现有car目录查找文件，找不到再查看是否构造
	worker_car_json_file := filepath.Join(os.Getenv("LOTUS_MINER_PATH"), "./retrieve_path.json")
	_, err := os.Stat(worker_car_json_file)
	if err != nil {
		return nil, xerrors.Errorf("don't have json file of car path")
	}
	byteValue, err := ioutil.ReadFile(worker_car_json_file)
	if err != nil {
		return nil, xerrors.Errorf("can't read %+v, err: %+v", worker_car_json_file, err)
	}
	var car_dir CarDir
	json.Unmarshal(byteValue, &car_dir)

	db, _ := sql.Open("mysql", "root:sxxfilweb@(10.100.248.32:3306)/deal")
	defer db.Close()
	dberr := db.Ping()
	if dberr != nil {
		log.Errorf("数据库连接失败 %+v", dberr)                  //连接失败
	} else {
		log.Errorf("数据库连接成功")                             //连接成功
	}
	sql := fmt.Sprintf("SELECT db_car.car_path, db_car.data_cid FROM db_car, db_deal WHERE db_car.piece_cid = db_deal.piece_cid AND db_deal.deal_provider = '%+v' AND db_deal.piece_cid = '%+v' LIMIT 1", car_dir.Miner, pc)
	row := db.QueryRow(sql)
	var car_path, data_cid string
	row.Scan(&car_path, &data_cid)
	worker_car_path := ""
	if car_path != "" {
		_, car_name := filepath.Split(car_path)
		for _, curdir := range car_dir.Dir {
			gen_path := filepath.Join(curdir, car_name)
			_, err = os.Stat(gen_path)
			if err == nil {
				worker_car_path = gen_path
				shell := fmt.Sprintf("sudo chmod 666 %+v", worker_car_path)
				cmd := exec.Command("sh", "-c", shell)
				cmd.Run()
				break
			}
		}
	}

	// 找不到现有car文件，尝试构造
	if worker_car_path == "" {
		if data_cid == "" {
			// return nil, xerrors.Errorf("can't read car file: %w", err)
			log.Errorf("mysql %+v", data_cid)
		}

		var quickcardir = os.Getenv("SXX_QUICKBUILD_DIR")

		dirs, _ := ioutil.ReadDir(quickcardir)
		quickbuild := false
		for _, f := range dirs {
			if strings.Contains(car_path, f.Name()) {
				worker_car_path = quickcardir + "/" + data_cid + ".car"
				quickbuild = true
				break
			}
		}

		// 找不到则使用原方法
		if !quickbuild {
			r, err := p.tryReadUnsealedPiece(ctx, pc, sector, pieceOffset, size)
			return r, err
		}

		err = p.GenerateCarFile(worker_car_path, data_cid)
		if err !=nil {
			return nil, xerrors.Errorf("can't read car file: %w", err)
		}
	}

	// 2k测试专用
	// data_cid = "bafybeidd5nbn644m3sjwnu7kbrtvk57ez3co7w7onsm4nc25cfcck24q7u"
	// worker_car_path = "/home/user/data/car/test.car"

	pr, err := (&pieceReader{
		ctx: ctx,
		getReader: func(ctx context.Context, startOffset uint64) (io.ReadCloser, error) {

			log.Errorf("zlin : 读取%+v文件", worker_car_path)

			content, err := os.Open(worker_car_path)
			if err != nil {
				return nil, xerrors.Errorf("test read car file: %w", err)
			}

			content.Seek(int64(startOffset), io.SeekStart)

			return struct {
				io.Reader
				io.Closer
			}{
				Reader: content,
				Closer: funcCloser(func() error {
					return content.Close()
				}),
			}, nil
		},
		len:      size,
		onClose:  cancel,
		pieceCid: pc,
	}).init()
	if err != nil || pr == nil { // pr == nil to make sure we don't return typed nil
		cancel()
		return nil, err
	}

	return pr, err
}

func (p *pieceProvider) GenerateCarFile(path string, data_cid string) error {
	_, err := os.Stat(path)
	if err != nil {
		log.Infow("创建car文件")
		err = p.createCar(data_cid)
		if err != nil {
			log.Errorf("创建car文件失败: %+v",err)
			return err
		}
	}
	log.Infow("创建car文件成功")
	return nil
}

func (p *pieceProvider) createCar(cid string) error {
	ssh := SshManager{
		Host:     "10.7.3.14",
		Username: "worker",
		Password: "WOPloong",
	}

	err := ssh.Open()
	if err != nil {
		return err
	}
	defer ssh.Close()
	cmd := fmt.Sprintf("cd ~/retrieve && ./car --cid %s", cid)
	s, err := ssh.Execute(cmd)
	if err != nil {
		fmt.Println(s)
		return err
	}
	return nil
}

func (ssh *SshManager) Execute(cmd string) (string, error) {
	session, err := ssh.client.NewSession()
	if err != nil {
		return "", err
	}
	defer session.Close()
	buffer, err := session.CombinedOutput(cmd)
	if err != nil {
		return "", err
	}
	return string(buffer), err
}

func (ssh *SshManager) Open() error {
	if ssh.Port == 0 {
		ssh.Port = 22
	}
	config := gossh.ClientConfig{
		User: ssh.Username,
		Auth: []gossh.AuthMethod{gossh.Password(ssh.Password)},
		HostKeyCallback: func(hostname string, remote net.Addr, key gossh.PublicKey) error {
			return nil
		},
		Timeout: 10 * time.Second,
	}
	addr := fmt.Sprintf("%s:%d", ssh.Host, ssh.Port)
	client, err := gossh.Dial("tcp", addr, &config)
	if err != nil {
		return err
	}
	ssh.client = client
	return nil
}

func (ssh *SshManager) Close() {
	ssh.client.Close()
}

// tryReadUnsealedPiece will try to read the unsealed piece from an existing unsealed sector file for the given sector from any worker that has it.
// It will NOT try to schedule an Unseal of a sealed sector file for the read.
//
// Returns a nil reader if the piece does NOT exist in any unsealed file or there is no unsealed file for the given sector on any of the workers.
func (p *pieceProvider) tryReadUnsealedPiece(ctx context.Context, pc cid.Cid, sector storiface.SectorRef, pieceOffset storiface.UnpaddedByteIndex, size abi.UnpaddedPieceSize) (mount.Reader, error) {
	// acquire a lock purely for reading unsealed sectors
	ctx, cancel := context.WithCancel(ctx)
	if err := p.index.StorageLock(ctx, sector.ID, storiface.FTUnsealed, storiface.FTNone); err != nil {
		cancel()
		return nil, xerrors.Errorf("acquiring read sector lock: %w", err)
	}

	// Reader returns a reader getter for an unsealed piece at the given offset in the given sector.
	// The returned reader will be nil if none of the workers has an unsealed sector file containing
	// the unsealed piece.
	rg, err := p.storage.Reader(ctx, sector, abi.PaddedPieceSize(pieceOffset.Padded()), size.Padded())
	if err != nil {
		cancel()
		log.Debugf("did not get storage reader;sector=%+v, err:%s", sector.ID, err)
		return nil, err
	}
	if rg == nil {
		cancel()
		return nil, nil
	}

	buf := make([]byte, fr32.BufSize(size.Padded()))

	pr, err := (&pieceReader{
		ctx: ctx,
		getReader: func(ctx context.Context, startOffset uint64) (io.ReadCloser, error) {
			log.Errorf("zlin startOffset %+v", startOffset)
			startOffsetAligned := storiface.UnpaddedByteIndex(startOffset / 127 * 127) // floor to multiple of 127
			log.Errorf("zlin startOffsetAligned %+v", startOffsetAligned)

			r, err := rg(startOffsetAligned.Padded())
			if err != nil {
				return nil, xerrors.Errorf("getting reader at +%d: %w", startOffsetAligned, err)
			}

			upr, err := fr32.NewUnpadReaderBuf(r, size.Padded(), buf)
			if err != nil {
				r.Close() // nolint
				return nil, xerrors.Errorf("creating unpadded reader: %w", err)
			}

			bir := bufio.NewReaderSize(upr, 127)
			if startOffset > uint64(startOffsetAligned) {
				if _, err := bir.Discard(int(startOffset - uint64(startOffsetAligned))); err != nil {
					r.Close() // nolint
					return nil, xerrors.Errorf("discarding bytes for startOffset: %w", err)
				}
			}

			return struct {
				io.Reader
				io.Closer
			}{
				Reader: bir,
				Closer: funcCloser(func() error {
					return r.Close()
				}),
			}, nil
		},
		len:      size,
		onClose:  cancel,
		pieceCid: pc,
	}).init()
	if err != nil || pr == nil { // pr == nil to make sure we don't return typed nil
		cancel()
		return nil, err
	}

	return pr, err
}

type funcCloser func() error

func (f funcCloser) Close() error {
	return f()
}

var _ io.Closer = funcCloser(nil)

// ReadPiece is used to read an Unsealed piece at the given offset and of the given size from a Sector
// If an Unsealed sector file exists with the Piece Unsealed in it, we'll use that for the read.
// Otherwise, we will Unseal a Sealed sector file for the given sector and read the Unsealed piece from it.
// If we do NOT have an existing unsealed file  containing the given piece thus causing us to schedule an Unseal,
// the returned boolean parameter will be set to true.
// If we have an existing unsealed file containing the given piece, the returned boolean will be set to false.
func (p *pieceProvider) ReadPiece(ctx context.Context, sector storiface.SectorRef, pieceOffset storiface.UnpaddedByteIndex, size abi.UnpaddedPieceSize, ticket abi.SealRandomness, unsealed cid.Cid) (mount.Reader, bool, error) {
	if err := pieceOffset.Valid(); err != nil {
		return nil, false, xerrors.Errorf("pieceOffset is not valid: %w", err)
	}
	if err := size.Validate(); err != nil {
		return nil, false, xerrors.Errorf("size is not a valid piece size: %w", err)
	}

	//r, err := p.tryReadUnsealedPiece(ctx, unsealed, sector, pieceOffset, size)
	r, err := p.tryReadUnsealedPieceOfSxx(ctx, unsealed, sector, pieceOffset, size)

	if xerrors.Is(err, storiface.ErrSectorNotFound) {
		log.Debugf("no unsealed sector file with unsealed piece, sector=%+v, pieceOffset=%d, size=%d", sector, pieceOffset, size)
		err = nil
	}
	if err != nil {
		log.Errorf("returning error from ReadPiece:%s", err)
		return nil, false, err
	}

	var uns bool

	if r == nil {
		// a nil reader means that none of the workers has an unsealed sector file
		// containing the unsealed piece.
		// we now need to unseal a sealed sector file for the given sector to read the unsealed piece from it.
		uns = true
		commd := &unsealed
		if unsealed == cid.Undef {
			commd = nil
		}
		if err := p.uns.SectorsUnsealPiece(ctx, sector, pieceOffset, size, ticket, commd); err != nil {
			log.Errorf("failed to SectorsUnsealPiece: %s", err)
			return nil, false, xerrors.Errorf("unsealing piece: %w", err)
		}

		log.Debugf("unsealed a sector file to read the piece, sector=%+v, pieceOffset=%d, size=%d", sector, pieceOffset, size)

		r, err = p.tryReadUnsealedPiece(ctx, unsealed, sector, pieceOffset, size)
		if err != nil {
			log.Errorf("failed to tryReadUnsealedPiece after SectorsUnsealPiece: %s", err)
			return nil, true, xerrors.Errorf("read after unsealing: %w", err)
		}
		if r == nil {
			log.Errorf("got no reader after unsealing piece")
			return nil, true, xerrors.Errorf("got no reader after unsealing piece")
		}
		log.Debugf("got a reader to read unsealed piece, sector=%+v, pieceOffset=%d, size=%d", sector, pieceOffset, size)
	} else {
		log.Debugf("unsealed piece already exists, no need to unseal, sector=%+v, pieceOffset=%d, size=%d", sector, pieceOffset, size)
	}

	log.Debugf("returning reader to read unsealed piece, sector=%+v, pieceOffset=%d, size=%d", sector, pieceOffset, size)

	return r, uns, nil
}
