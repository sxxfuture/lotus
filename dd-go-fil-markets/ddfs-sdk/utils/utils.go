//DD add
package utils

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/filecoin-project/go-state-types/abi"
)

func SectorName(sid abi.SectorID) string {
	return fmt.Sprintf("s-t0%d-%d", sid.Miner, sid.Number)
}

func SleepRandT(wait time.Duration) {
	time.Sleep(time.Duration(rand.Int63n(int64(wait))) + wait/2)
}
