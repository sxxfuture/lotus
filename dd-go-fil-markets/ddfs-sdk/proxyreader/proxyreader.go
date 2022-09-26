//DD add
package proxyreader

import (
	"errors"
	"github.com/filecoin-project/go-fil-markets/ddfs-sdk/api"
	"io"
	"math/rand"
	"strings"
	"time"

	"github.com/filecoin-project/go-fil-markets/ddfs-sdk/http_response"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("ddfs-proxyreader")

type ReaderProxy struct {
	newRemoteFile  func() (io.ReadCloser, uint64, error)
	remoteFile     io.ReadCloser
	remoteFileSize uint64
	readn          int
	errLimit       int
	errCount       int
	errWait        time.Duration
	errResult      error
}

func NewReaderProxy(newReader func() (io.ReadCloser, uint64, error), errLimit int, errWait time.Duration) (api.RemoteFile, error) {
	if errLimit < 1 {
		panic("errLimit must be grather than 0")
	}
	if errWait < time.Microsecond {
		errWait = time.Microsecond
	}

	readProxy := &ReaderProxy{
		newRemoteFile: newReader,
		readn:         0,
		errLimit:      errLimit,
		errCount:      0,
		errWait:       errWait,
		errResult:     nil,
	}

	remoteFile, remoteFileSize, err := readProxy.try()
	if err != nil {
		return nil, err
	}
	readProxy.remoteFile = remoteFile
	readProxy.remoteFileSize = remoteFileSize
	return readProxy, nil
}

func (pr *ReaderProxy) try() (io.ReadCloser, uint64, error) {
	var (
		err            error
		remoteFile     io.ReadCloser
		remoteFileSize uint64
	)

	_ = pr.Close()

	for {
		remoteFile, remoteFileSize, err = pr.newRemoteFile()
		switch {
		case err == nil:
			return remoteFile, remoteFileSize, nil
		case errors.Is(err, io.EOF),
			http_response.Is500Err(err),
			http_response.IsUnexpected(err),
			http_response.Is(err, http_response.InsufficientRemainingTime),
			strings.Contains(err.Error(), "connect"):
		default:
			log.Errorf("[DD] newRemoteFile failed err: %v", err)
			return nil, 0, err
		}

		log.Warnf("[DD] newRemoteFile failed err: %v", err)
		pr.errCount++
		if pr.errCount >= pr.errLimit {
			log.Errorf("[DD] newRemoteFile failed too many time,err: %v", err)
			return nil, 0, err
		}

		{
			wait := pr.errWait / 1000 * time.Duration(rand.Intn(1000)+500)
			log.Warnf("[DD] try to read again %vth time after %v,readCount:%v", pr.errCount, wait, pr.readn)
			time.Sleep(wait)
		}
	}
}

func (pr *ReaderProxy) Read(p []byte) (n int, err error) {
	for {
		n, err = pr.remoteFile.Read(p)
		switch {
		case err == nil, errors.Is(err, io.EOF):
			pr.readn += n
			return n, err
		}
		n = 0

	TRY:
		for {
			var remoteFile io.ReadCloser
			remoteFile, _, err = pr.try()
			if err != nil {
				return 0, err
			}
			pr.remoteFile = remoteFile
			limitReader := io.LimitReader(pr.remoteFile, int64(pr.readn))
			rubbish := make([]byte, 1024)
			for {
				_, err = limitReader.Read(rubbish)
				if err != nil {
					if errors.Is(err, io.EOF) {
						if limitReader.(*io.LimitedReader).N > 0 {
							log.Errorf("[DD] an impossible mistake")
							return n, err
						}
						break TRY
					}
					log.Warnf("[DD] read data failed err: %v", err)
					break
				}
			}
		}
	}
}

func (pr *ReaderProxy) Close() error {
	if pr.remoteFile != nil {
		return pr.remoteFile.Close()
	}
	return nil
}

func (pr *ReaderProxy) Size() (uint64, error) {
	if pr.errResult != nil {
		return 0, pr.errResult
	}
	return pr.remoteFileSize, nil
}
