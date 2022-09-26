//DD add
package addpiece

import (
	"context"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

func HaveExtInfo(ctx context.Context) bool {
	if ctx.Value("pieceCID") != nil && ctx.Value("remoteFileUrl") != nil {
		return true
	}
	return false
}

func DDCpCtx(from context.Context, to *context.Context) {
	if pieceCid := from.Value("pieceCID"); pieceCid != nil {
		*to = context.WithValue(*to, "pieceCID", pieceCid)
	}
	if remoteFileUrl := from.Value("remoteFileUrl"); remoteFileUrl != nil {
		*to = context.WithValue(*to, "remoteFileUrl", remoteFileUrl)
	}
	if fileHash := from.Value("fileHash"); fileHash != nil {
		*to = context.WithValue(*to, "fileHash", fileHash)
	}
}

func GetExtInfo(ctx context.Context) map[string]interface{} {
	info := map[string]interface{}{}
	if pieceCid := ctx.Value("pieceCID"); pieceCid != nil {
		info["pieceCID"] = pieceCid
	}
	if remoteFileUrl := ctx.Value("remoteFileUrl"); remoteFileUrl != nil {
		info["remoteFileUrl"] = remoteFileUrl
	}
	if fileHash := ctx.Value("fileHash"); fileHash != nil {
		info["fileHash"] = fileHash
	}
	return info
}

func SetCtxFromExtInfo(ctx *context.Context, extInfo map[string]interface{}) {
	for k, v := range extInfo {
		*ctx = context.WithValue(*ctx, k, v)
	}
}

var WaitBeforeAddPiece = func() func() {
	var wait_time time.Duration
	if interval := func() int64 {
		interval := os.Getenv("WAIT_BEFORE_ADDPIECE")
		if interval == "" {
			return 0
		}
		num, err := strconv.Atoi(interval)
		if err != nil {
			log.Errorf("env WAIT_BEFORE_ADDPIECE err:[%v]", err)
			return 0
		}
		return int64(num)
	}(); interval != 0 {
		wait_time = time.Second * time.Duration(interval)
	} else {
		wait_time = time.Second
	}

	var waitForAddPiece int64
	go func() {
		for {
			time.Sleep(time.Second)
			if wait := atomic.LoadInt64(&waitForAddPiece); wait > 0 {
				if wait >= int64(time.Second) {
					atomic.AddInt64(&waitForAddPiece, -int64(time.Second))
				} else {
					atomic.StoreInt64(&waitForAddPiece, 0)
				}
			}
		}
	}()

	return func() {
		n := atomic.AddInt64(&waitForAddPiece, int64(wait_time))
		time.Sleep(time.Duration(n - int64(wait_time)))
		return
	}
}()
