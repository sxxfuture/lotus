package miner

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	logging "github.com/ipfs/go-log/v2"
	"go.opencensus.io/trace"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/go-state-types/proof"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/actors/builtin"
	"github.com/filecoin-project/lotus/chain/actors/policy"
	"github.com/filecoin-project/lotus/chain/gen"
	"github.com/filecoin-project/lotus/chain/gen/slashfilter"
	lrand "github.com/filecoin-project/lotus/chain/rand"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/wallet/key"
	cliutil "github.com/filecoin-project/lotus/cli/util"
	"github.com/filecoin-project/lotus/journal"
	"github.com/filecoin-project/lotus/lib/sigs"

	"github.com/filecoin-project/go-state-types/network"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("miner")

// Journal event types.
const (
	evtTypeBlockMined = iota
)

// waitFunc is expected to pace block mining at the configured network rate.
//
// baseTime is the timestamp of the mining base, i.e. the timestamp
// of the tipset we're planning to construct upon.
//
// Upon each mining loop iteration, the returned callback is called reporting
// whether we mined a block in this round or not.
type waitFunc func(ctx context.Context, baseTime uint64) (func(bool, abi.ChainEpoch, error), abi.ChainEpoch, error)

func randTimeOffset(width time.Duration) time.Duration {
	buf := make([]byte, 8)
	rand.Reader.Read(buf) //nolint:errcheck
	val := time.Duration(binary.BigEndian.Uint64(buf) % uint64(width))

	return val - (width / 2)
}

// NewMiner instantiates a miner with a concrete WinningPoStProver and a miner
// address (which can be different from the worker's address).
func NewMiner(api v1api.FullNode, epp gen.WinningPoStProver, addr address.Address, sf *slashfilter.SlashFilter, j journal.Journal) *Miner {
	arc, err := lru.NewARC[abi.ChainEpoch, bool](10000)
	if err != nil {
		panic(err)
	}

	return &Miner{
		api:     api,
		epp:     epp,
		address: addr,
		waitFunc: func(ctx context.Context, baseTime uint64) (func(bool, abi.ChainEpoch, error), abi.ChainEpoch, error) {
			// wait around for half the block time in case other parents come in
			//
			// if we're mining a block in the past via catch-up/rush mining,
			// such as when recovering from a network halt, this sleep will be
			// for a negative duration, and therefore **will return
			// immediately**.
			//
			// the result is that we WILL NOT wait, therefore fast-forwarding
			// and thus healing the chain by backfilling it with null rounds
			// rapidly.
			deadline := baseTime + build.PropagationDelaySecs
			baseT := time.Unix(int64(deadline), 0)

			baseT = baseT.Add(randTimeOffset(time.Second))

			build.Clock.Sleep(build.Clock.Until(baseT))

			return func(bool, abi.ChainEpoch, error) {}, 0, nil
		},

		sf:                sf,
		minedBlockHeights: arc,
		evtTypes: [...]journal.EventType{
			evtTypeBlockMined: j.RegisterEventType("miner", "block_mined"),
		},
		journal: j,
	}
}

// Miner encapsulates the mining processes of the system.
//
// Refer to the godocs on mineOne and mine methods for more detail.
type Miner struct {
	api v1api.FullNode

	epp gen.WinningPoStProver

	lk       sync.Mutex
	address  address.Address
	stop     chan struct{}
	stopping chan struct{}

	waitFunc waitFunc

	// lastWork holds the last MiningBase we built upon.
	lastWork *MiningBase

	sf *slashfilter.SlashFilter
	// minedBlockHeights is a safeguard that caches the last heights we mined.
	// It is consulted before publishing a newly mined block, for a sanity check
	// intended to avoid slashings in case of a bug.
	minedBlockHeights *lru.ARCCache[abi.ChainEpoch, bool]

	evtTypes [1]journal.EventType
	journal  journal.Journal
}

// Address returns the address of the miner.
func (m *Miner) Address() address.Address {
	m.lk.Lock()
	defer m.lk.Unlock()

	return m.address
}

// Start starts the mining operation. It spawns a goroutine and returns
// immediately. Start is not idempotent.
func (m *Miner) Start(_ context.Context) error {
	m.lk.Lock()
	defer m.lk.Unlock()
	if m.stop != nil {
		return fmt.Errorf("miner already started")
	}
	m.stop = make(chan struct{})
	//go m.mine(context.TODO())

	// change by sxx
	if _, ok := os.LookupEnv("LOTUS_WNPOST"); ok {
		go m.mine(context.TODO())
	} else {
		log.Warnf("This miner will be disable minning block function.")
	}
	// end
	return nil
}

// Stop stops the mining operation. It is not idempotent, and multiple adjacent
// calls to Stop will fail.
func (m *Miner) Stop(ctx context.Context) error {
	m.lk.Lock()

	m.stopping = make(chan struct{})
	stopping := m.stopping
	close(m.stop)

	m.lk.Unlock()

	select {
	case <-stopping:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Miner) niceSleep(d time.Duration) bool {
	select {
	case <-build.Clock.After(d):
		return true
	case <-m.stop:
		log.Infow("received interrupt while trying to sleep in mining cycle")
		return false
	}
}

// mine runs the mining loop. It performs the following:
//
//  1. Queries our current best currently-known mining candidate (tipset to
//     build upon).
//  2. Waits until the propagation delay of the network has elapsed (currently
//     6 seconds). The waiting is done relative to the timestamp of the best
//     candidate, which means that if it's way in the past, we won't wait at
//     all (e.g. in catch-up or rush mining).
//  3. After the wait, we query our best mining candidate. This will be the one
//     we'll work with.
//  4. Sanity check that we _actually_ have a new mining base to mine on. If
//     not, wait one epoch + propagation delay, and go back to the top.
//  5. We attempt to mine a block, by calling mineOne (refer to godocs). This
//     method will either return a block if we were eligible to mine, or nil
//     if we weren't.
//     6a. If we mined a block, we update our state and push it out to the network
//     via gossipsub.
//     6b. If we didn't mine a block, we consider this to be a nil round on top of
//     the mining base we selected. If other miner or miners on the network
//     were eligible to mine, we will receive their blocks via gossipsub and
//     we will select that tipset on the next iteration of the loop, thus
//     discarding our null round.
func (m *Miner) mine(ctx context.Context) {
	ctx, span := trace.StartSpan(ctx, "/mine")
	defer span.End()

	go m.doWinPoStWarmup(ctx)

	var lastBase MiningBase
minerLoop:
	for {
		ctx := cliutil.OnSingleNode(ctx)

		select {
		case <-m.stop:
			stopping := m.stopping
			m.stop = nil
			m.stopping = nil
			close(stopping)
			return

		default:
		}

		var base *MiningBase
		var onDone func(bool, abi.ChainEpoch, error)
		var injectNulls abi.ChainEpoch
		var addr string
		start := time.Now()
		for {
			// prebase, err := m.GetBestMiningCandidate(ctx)
			var prebase *MiningBase
			var err error
			prebase, addr, err = m.GetBestMiningCandidateOfSxx(ctx)
			// prebase, err = m.GetBestMiningCandidate(ctx)
			if err != nil {
				log.Errorf("failed to get best mining candidate: %s", err)
				if !m.niceSleep(time.Second * 5) {
					continue minerLoop
				}
				continue
			}

			if os.Getenv("FULLNODE_API_INFO_OF_SXX") == "" {
				log.Warnf("FULLNODE_API_INFO_OF_SXX if nil")
			}

			if base != nil && base.TipSet.Height() == prebase.TipSet.Height() && base.NullRounds == prebase.NullRounds {
				base = prebase
				// add by pan compare tipset hight
				end := time.Now()
				if end.Unix()-start.Unix() <= 10 {
					best := GetBestTipSet(ctx)
					if best == nil || base.TipSet.Height() < best.Height() {
						var bestHeight abi.ChainEpoch
						if best != nil {
							bestHeight = best.Height()
						}
						log.Warn("waiting for tipset", fmt.Sprintf(" %s,%s,%d,%d,%d",
							start.Format("2006-01-02 03:04:05"),
							end.Format("2006-01-02 03:04:05"),
							base.TipSet.Height(),
							bestHeight,
							end.Unix()-start.Unix()))
						continue
					}
				} else {
					log.Warn("waiting for tipset timeout")
				}
				// end
				break
			}
			if base != nil {
				onDone(false, 0, nil)
			}

			// TODO: need to change the orchestration here. the problem is that
			// we are waiting *after* we enter this loop and selecta mining
			// candidate, which is almost certain to change in multiminer
			// tests. Instead, we should block before entering the loop, so
			// that when the test 'MineOne' function is triggered, we pull our
			// best mining candidate at that time.

			// Wait until propagation delay period after block we plan to mine on
			onDone, injectNulls, err = m.waitFunc(ctx, prebase.TipSet.MinTimestamp())
			if err != nil {
				log.Error(err)
				continue
			}

			// just wait for the beacon entry to become available before we select our final mining base
			// _, err = m.api.StateGetBeaconEntry(ctx, prebase.TipSet.Height()+prebase.NullRounds+1)
			if os.Getenv("FULLNODE_API_INFO_OF_SXX") != "" {
				_, err = m.GetStateGetBeaconEntryOfSxx(ctx, prebase.TipSet.Height()+prebase.NullRounds+1, addr)
			} else {
				_, err = m.api.StateGetBeaconEntry(ctx, prebase.TipSet.Height()+prebase.NullRounds+1)
			}
			if err != nil {
				log.Errorf("failed getting beacon entry: %s", err)
				if !m.niceSleep(time.Second) {
					continue minerLoop
				}
				continue
			}

			base = prebase
		}

		base.NullRounds += injectNulls // testing

		if base.TipSet.Equals(lastBase.TipSet) && lastBase.NullRounds == base.NullRounds {
			log.Warnf("BestMiningCandidate from the previous round: %s (nulls:%d)", lastBase.TipSet.Cids(), lastBase.NullRounds)
			if !m.niceSleep(time.Duration(build.BlockDelaySecs) * time.Second) {
				continue minerLoop
			}
			continue
		}

		// b, err := m.mineOne(ctx, base)
		var b *types.BlockMsg
		var err error
		if os.Getenv("FULLNODE_API_INFO_OF_SXX") != "" {
			b, err = m.mineOneOfSxx(ctx, base, addr)
		} else {
			b, err = m.mineOne(ctx, base)
		}
		if err != nil {
			log.Errorf("mining block failed: %+v", err)
			if !m.niceSleep(time.Second) {
				continue minerLoop
			}
			onDone(false, 0, err)
			continue
		}
		lastBase = *base

		var h abi.ChainEpoch
		if b != nil {
			h = b.Header.Height
		}
		onDone(b != nil, h, nil)

		if b != nil {
			m.journal.RecordEvent(m.evtTypes[evtTypeBlockMined], func() interface{} {
				return map[string]interface{}{
					"parents":   base.TipSet.Cids(),
					"nulls":     base.NullRounds,
					"epoch":     b.Header.Height,
					"timestamp": b.Header.Timestamp,
					"cid":       b.Header.Cid(),
				}
			})

			btime := time.Unix(int64(b.Header.Timestamp), 0)
			now := build.Clock.Now()
			switch {
			case btime == now:
				// block timestamp is perfectly aligned with time.
			case btime.After(now):
				if !m.niceSleep(build.Clock.Until(btime)) {
					log.Warnf("received interrupt while waiting to broadcast block, will shutdown after block is sent out")
					build.Clock.Sleep(build.Clock.Until(btime))
				}
			default:
				log.Warnw("mined block in the past",
					"block-time", btime, "time", build.Clock.Now(), "difference", build.Clock.Since(btime))
			}

			if err := m.sf.MinedBlock(ctx, b.Header, base.TipSet.Height()+base.NullRounds); err != nil {
				log.Errorf("<!!> SLASH FILTER ERROR: %s", err)
				if os.Getenv("LOTUS_MINER_NO_SLASHFILTER") != "_yes_i_know_i_can_and_probably_will_lose_all_my_fil_and_power_" {
					continue
				}
			}

			if _, ok := m.minedBlockHeights.Get(b.Header.Height); ok {
				log.Warnw("Created a block at the same height as another block we've created", "height", b.Header.Height, "miner", b.Header.Miner, "parents", b.Header.Parents)
				continue
			}

			m.minedBlockHeights.Add(b.Header.Height, true)

			// if err := m.api.SyncSubmitBlock(ctx, b); err != nil {
			// 	log.Errorf("failed to submit newly mined block: %+v", err)
			// }
			if os.Getenv("FULLNODE_API_INFO_OF_SXX") != "" {
				if err := m.GetSyncSubmitBlockOfSxx(ctx, b, addr); err != nil {
					log.Errorf("failed to submit newly mined block: %+v", err)
				}
			} else {
				if err := m.api.SyncSubmitBlock(ctx, b); err != nil {
					log.Errorf("failed to submit newly mined block: %+v", err)
				}
			}
		} else {
			base.NullRounds++

			// Wait until the next epoch, plus the propagation delay, so a new tipset
			// has enough time to form.
			//
			// See:  https://github.com/filecoin-project/lotus/issues/1845
			nextRound := time.Unix(int64(base.TipSet.MinTimestamp()+build.BlockDelaySecs*uint64(base.NullRounds))+int64(build.PropagationDelaySecs), 0)

			select {
			case <-build.Clock.After(build.Clock.Until(nextRound)):
			case <-m.stop:
				stopping := m.stopping
				m.stop = nil
				m.stopping = nil
				close(stopping)
				return
			}
		}
	}
}

// MiningBase is the tipset on top of which we plan to construct our next block.
// Refer to godocs on GetBestMiningCandidate.
type MiningBase struct {
	TipSet     *types.TipSet
	NullRounds abi.ChainEpoch
}

// 检查数组 arr1 是否包含 arr2
func containsArray(arr1 []cid.Cid, arr2 []cid.Cid) bool {
	freq := make(map[cid.Cid]int)
	for _, tipset := range arr1 {
		freq[tipset]++
	}
	for _, tipset := range arr2 {
		if freq[tipset] == 0 {
			return false
		}
		freq[tipset]--
	}
	return true
}

func findMostFrequent(ctx context.Context, fullnodelist []v1api.FullNode, addrs []string) (*types.TipSet, string) {
	btslist := make([]*types.TipSet, 0)
	addrlist := make([]string, 0)

	// 改成并行
	wg := sync.WaitGroup{}
	mutex := sync.Mutex{}
	for i, fullnode := range fullnodelist {
		wg.Add(1)
		go func(fullnode v1api.FullNode, i int) {
			defer wg.Done()
			bts, err := fullnode.ChainHead(ctx)
			if err != nil {
				log.Warn("fail to get chainhead %+v", err)
				return
			}
			mutex.Lock()
			btslist = append(btslist, bts)
			addrlist = append(addrlist, addrs[i])
			mutex.Unlock()
		}(fullnode, i)
	}
	wg.Wait()
	// end

	if len(btslist) == 0 {
		return nil, ""
	}

	// 去掉高度低的tipset
	index := 0
	for {
		if len(btslist) <= 1 {
			break
		}
		if btslist[index+1].Height() > btslist[index].Height() {
			btslist = append(btslist[:index], btslist[index+1:]...)
			addrlist = append(addrlist[:index], addrlist[index+1:]...)
			index = 0
			continue
		}

		if btslist[index+1].Height() < btslist[index].Height() {
			btslist = append(btslist[:index+1], btslist[index+2:]...)
			addrlist = append(addrlist[:index+1], addrlist[index+2:]...)
			index = 0
			continue
		}
		index += 1
		if index+1 == len(btslist) {
			break
		}
	}

	// 拿到相同tipset的数量
	freq := make(map[*types.TipSet]int)
	mostFrequent := btslist[0]
	maxFreq := 1
	for _, bts := range btslist {
		next := false
		for k, _ := range freq {
			if k.Equals(bts) {
				freq[k]++
				next = true
				break
			}
		}
		if next {
			continue
		}
		freq[bts]++
	}

	// 找出相同最多的tipset
	for bts, num := range freq {
		if num >= maxFreq {
			maxFreq = num
			mostFrequent = bts
		}
	}

	// 在找到相同数最多的基础上，拿到包含且父tipset比其多的当前tipset
	for bts, _ := range freq {
		if containsArray(mostFrequent.Key().Cids(), bts.Key().Cids()) {
			continue
		}
		if containsArray(bts.Key().Cids(), mostFrequent.Key().Cids()) {
			mostFrequent = bts
		}
	}

	for i, v := range btslist {
		if v.Equals(mostFrequent) {
			return btslist[i], addrlist[i]
		}
	}

	return btslist[0], addrlist[0]
}

func (m *Miner) GetGoodBestMiningCandidate(ctx context.Context) (*types.TipSet, string, error) {
	cctx := cli.NewContext(nil, nil, nil)
	fullnodelist, ncloser, addrs, err := lcli.GetFullNodeAPIV1OfSxx(cctx)
	if err != nil {
		return nil, "", err
	}
	defer ncloser()

	ts, addr := findMostFrequent(ctx, fullnodelist, addrs)

	return ts, addr, nil
}

func (m *Miner) GetSyncSubmitBlockOfSxx(ctx context.Context, blk *types.BlockMsg, addr string) error {
	cctx := cli.NewContext(nil, nil, nil)
	fullnodelist, ncloser, addrs, err := lcli.GetFullNodeAPIV1OfSxx(cctx)
	if err != nil {
		return err
	}
	defer ncloser()

	for i, h := range addrs {
		if h == addr {
			return fullnodelist[i].SyncSubmitBlock(ctx, blk)
		}
	}

	return fullnodelist[0].SyncSubmitBlock(ctx, blk)
}

func (m *Miner) GetStateGetBeaconEntryOfSxx(ctx context.Context, epoch abi.ChainEpoch, addr string) (*types.BeaconEntry, error) {
	cctx := cli.NewContext(nil, nil, nil)
	fullnodelist, ncloser, addrs, err := lcli.GetFullNodeAPIV1OfSxx(cctx)
	if err != nil {
		return nil, err
	}
	defer ncloser()

	for i, h := range addrs {
		if h == addr {
			return fullnodelist[i].StateGetBeaconEntry(ctx, epoch)
		}
	}

	return fullnodelist[0].StateGetBeaconEntry(ctx, epoch)
}

func (m *Miner) GetChainTipSetWeightOfSxx(ctx context.Context, tsk types.TipSetKey, addr string) (types.BigInt, error) {
	cctx := cli.NewContext(nil, nil, nil)
	fullnodelist, ncloser, addrs, err := lcli.GetFullNodeAPIV1OfSxx(cctx)
	if err != nil {
		return types.NewInt(0), err
	}
	defer ncloser()

	for i, h := range addrs {
		if h == addr {
			return fullnodelist[i].ChainTipSetWeight(ctx, tsk)
		}
	}

	return fullnodelist[0].ChainTipSetWeight(ctx, tsk)
}

func (m *Miner) GetStateMinerPowerOfSxx(ctx context.Context, addr address.Address, tsk types.TipSetKey, a string) (*api.MinerPower, error) {
	cctx := cli.NewContext(nil, nil, nil)
	fullnodelist, ncloser, addrs, err := lcli.GetFullNodeAPIV1OfSxx(cctx)
	if err != nil {
		return nil, err
	}
	defer ncloser()

	for i, h := range addrs {
		if h == a {
			return fullnodelist[i].StateMinerPower(ctx, addr, tsk)
		}
	}

	return fullnodelist[0].StateMinerPower(ctx, addr, tsk)
}

func (m *Miner) GetMinerGetBaseInfoOfSxx(ctx context.Context, maddr address.Address, epoch abi.ChainEpoch, tsk types.TipSetKey, addr string) (*api.MiningBaseInfo, error) {
	cctx := cli.NewContext(nil, nil, nil)
	fullnodelist, ncloser, addrs, err := lcli.GetFullNodeAPIV1OfSxx(cctx)
	if err != nil {
		return nil, err
	}
	defer ncloser()

	for i, h := range addrs {
		if h == addr {
			return fullnodelist[i].MinerGetBaseInfo(ctx, maddr, epoch, tsk)
		}
	}

	return fullnodelist[0].MinerGetBaseInfo(ctx, maddr, epoch, tsk)
}

func (m *Miner) GetStateNetworkVersionOfSxx(ctx context.Context, tsk types.TipSetKey, addr string) (network.Version, error) {
	cctx := cli.NewContext(nil, nil, nil)
	fullnodelist, ncloser, addrs, err := lcli.GetFullNodeAPIV1OfSxx(cctx)
	if err != nil {
		return network.VersionMax, err
	}
	defer ncloser()

	for i, h := range addrs {
		if h == addr {
			return fullnodelist[i].StateNetworkVersion(ctx, tsk)
		}
	}

	return fullnodelist[0].StateNetworkVersion(ctx, tsk)
}

func (m *Miner) GetMpoolSelectOfSxx(ctx context.Context, tsk types.TipSetKey, ticketQuality float64, addr string) ([]*types.SignedMessage, error) {
	cctx := cli.NewContext(nil, nil, nil)
	fullnodelist, ncloser, addrs, err := lcli.GetFullNodeAPIV1OfSxx(cctx)
	if err != nil {
		return nil, err
	}
	defer ncloser()

	for i, h := range addrs {
		if h == addr {
			return fullnodelist[i].MpoolSelect(ctx, tsk, ticketQuality)
		}
	}

	return fullnodelist[0].MpoolSelect(ctx, tsk, ticketQuality)
}

func WalletSign(ctx context.Context, msg []byte) (*crypto.Signature, error) {

	inpdata, err := os.ReadFile(path.Join(os.Getenv("LOTUS_MINER_PATH"), "PrivateKey"))
	if err != nil {
		return nil, err
	}

	data, err := hex.DecodeString(strings.TrimSpace(string(inpdata)))
	if err != nil {
		return nil, err
	}

	var ki types.KeyInfo
	if err := json.Unmarshal(data, &ki); err != nil {
		return nil, err
	}

	return sigs.Sign(key.ActSigType(ki.Type), ki.PrivateKey, msg)
}

func (m *Miner) GetMinerCreateBlockOfSxx(ctx context.Context, bt *api.BlockTemplate, addr string) (*types.BlockMsg, error) {
	cctx := cli.NewContext(nil, nil, nil)
	fullnodelist, ncloser, addrs, err := lcli.GetFullNodeAPIV1OfSxx(cctx)
	if err != nil {
		return nil, err
	}
	defer ncloser()

	for i, h := range addrs {
		if h == addr {
			// return fullnodelist[i].MinerCreateBlock(ctx, bt)
			bm, err := fullnodelist[i].MinerCreateBlockOfSxx(ctx, bt)
			if err != nil {
				return nil, err
			}

			nosigbytes, err := bm.Header.SigningBytes()
			if err != nil {
				return nil, xerrors.Errorf("failed to get signing bytes for block: %w", err)
			}

			sig, err := WalletSign(ctx, nosigbytes)
			if err != nil {
				return nil, xerrors.Errorf("failed to sign new block: %w", err)
			}
			bm.Header.BlockSig = sig

			return bm, nil

		}
	}

	bm, err := fullnodelist[0].MinerCreateBlockOfSxx(ctx, bt)
	if err != nil {
		return nil, err
	}

	nosigbytes, err := bm.Header.SigningBytes()
	if err != nil {
		return nil, xerrors.Errorf("failed to get signing bytes for block: %w", err)
	}

	sig, err := WalletSign(ctx, nosigbytes)
	if err != nil {
		return nil, xerrors.Errorf("failed to sign new block: %w", err)
	}
	bm.Header.BlockSig = sig

	return bm, nil
}

// GetBestMiningCandidate implements the fork choice rule from a miner's
// perspective.
//
// It obtains the current chain head (HEAD), and compares it to the last tipset
// we selected as our mining base (LAST). If HEAD's weight is larger than
// LAST's weight, it selects HEAD to build on. Else, it selects LAST.
func (m *Miner) GetBestMiningCandidate(ctx context.Context) (*MiningBase, error) {
	m.lk.Lock()
	defer m.lk.Unlock()

	bts, err := m.api.ChainHead(ctx)
	if err != nil {
		return nil, err
	}

	if m.lastWork != nil {
		if m.lastWork.TipSet.Equals(bts) {
			return m.lastWork, nil
		}

		btsw, err := m.api.ChainTipSetWeight(ctx, bts.Key())
		if err != nil {
			return nil, err
		}
		ltsw, err := m.api.ChainTipSetWeight(ctx, m.lastWork.TipSet.Key())
		if err != nil {
			m.lastWork = nil
			return nil, err
		}

		if types.BigCmp(btsw, ltsw) <= 0 {
			return m.lastWork, nil
		}
	}

	m.lastWork = &MiningBase{TipSet: bts}
	return m.lastWork, nil
}

func (m *Miner) GetBestMiningCandidateOfSxx(ctx context.Context) (*MiningBase, string, error) {
	m.lk.Lock()
	defer m.lk.Unlock()

	bts, addr, err := m.GetGoodBestMiningCandidate(ctx)
	if err != nil {
		return nil, "", err
	}

	if bts == nil {
		return nil, "", xerrors.Errorf("can't get good Best in FULLNODE_API_INFO_OF_SXX")
	}

	if m.lastWork != nil {
		if m.lastWork.TipSet.Equals(bts) {
			return m.lastWork, addr, nil
		}

		// btsw, err := m.api.ChainTipSetWeight(ctx, bts.Key())
		btsw, err := m.GetChainTipSetWeightOfSxx(ctx, bts.Key(), addr)
		if err != nil {
			return nil, "", err
		}
		// ltsw, err := m.api.ChainTipSetWeight(ctx, m.lastWork.TipSet.Key())
		ltsw, err := m.GetChainTipSetWeightOfSxx(ctx, m.lastWork.TipSet.Key(), addr)
		if err != nil {
			m.lastWork = nil
			return nil, "", err
		}

		if types.BigCmp(btsw, ltsw) <= 0 {
			return m.lastWork, addr, nil
		}
	}

	m.lastWork = &MiningBase{TipSet: bts}
	return m.lastWork, addr, nil
}

// mineOne attempts to mine a single block, and does so synchronously, if and
// only if we are eligible to mine.
//
// {hint/landmark}: This method coordinates all the steps involved in mining a
// block, including the condition of whether mine or not at all depending on
// whether we win the round or not.
//
// This method does the following:
//
//	1.
func (m *Miner) mineOne(ctx context.Context, base *MiningBase) (minedBlock *types.BlockMsg, err error) {
	log.Debugw("attempting to mine a block", "tipset", types.LogCids(base.TipSet.Cids()))
	tStart := build.Clock.Now()

	round := base.TipSet.Height() + base.NullRounds + 1

	// always write out a log
	var winner *types.ElectionProof
	var mbi *api.MiningBaseInfo
	var rbase types.BeaconEntry
	defer func() {

		var hasMinPower bool

		// mbi can be nil if we are deep in penalty and there are 0 eligible sectors
		// in the current deadline. If this case - put together a dummy one for reporting
		// https://github.com/filecoin-project/lotus/blob/v1.9.0/chain/stmgr/utils.go#L500-L502
		if mbi == nil {
			mbi = &api.MiningBaseInfo{
				NetworkPower:      big.NewInt(-1), // we do not know how big the network is at this point
				EligibleForMining: false,
				MinerPower:        big.NewInt(0), // but we do know we do not have anything eligible
			}

			// try to opportunistically pull actual power and plug it into the fake mbi
			if pow, err := m.api.StateMinerPower(ctx, m.address, base.TipSet.Key()); err == nil && pow != nil {
				hasMinPower = pow.HasMinPower
				mbi.MinerPower = pow.MinerPower.QualityAdjPower
				mbi.NetworkPower = pow.TotalPower.QualityAdjPower
			}
		}

		isLate := uint64(tStart.Unix()) > (base.TipSet.MinTimestamp() + uint64(base.NullRounds*builtin.EpochDurationSeconds) + build.PropagationDelaySecs)

		logStruct := []interface{}{
			"tookMilliseconds", (build.Clock.Now().UnixNano() - tStart.UnixNano()) / 1_000_000,
			"forRound", int64(round),
			"baseEpoch", int64(base.TipSet.Height()),
			"baseDeltaSeconds", uint64(tStart.Unix()) - base.TipSet.MinTimestamp(),
			"nullRounds", int64(base.NullRounds),
			"lateStart", isLate,
			"beaconEpoch", rbase.Round,
			"lookbackEpochs", int64(policy.ChainFinality), // hardcoded as it is unlikely to change again: https://github.com/filecoin-project/lotus/blob/v1.8.0/chain/actors/policy/policy.go#L180-L186
			"networkPowerAtLookback", mbi.NetworkPower.String(),
			"minerPowerAtLookback", mbi.MinerPower.String(),
			"isEligible", mbi.EligibleForMining,
			"isWinner", (winner != nil),
			"error", err,
		}

		if err != nil {
			log.Errorw("completed mineOne", logStruct...)
		} else if isLate || (hasMinPower && !mbi.EligibleForMining) {
			log.Warnw("completed mineOne", logStruct...)
		} else {
			log.Infow("completed mineOne", logStruct...)
		}
	}()

	mbi, err = m.api.MinerGetBaseInfo(ctx, m.address, round, base.TipSet.Key())
	if err != nil {
		err = xerrors.Errorf("failed to get mining base info: %w", err)
		return nil, err
	}
	if mbi == nil {
		return nil, nil
	}

	if !mbi.EligibleForMining {
		// slashed or just have no power yet
		return nil, nil
	}

	tPowercheck := build.Clock.Now()

	bvals := mbi.BeaconEntries
	rbase = mbi.PrevBeaconEntry
	if len(bvals) > 0 {
		rbase = bvals[len(bvals)-1]
	}

	ticket, err := m.computeTicket(ctx, &rbase, base, mbi)
	if err != nil {
		err = xerrors.Errorf("scratching ticket failed: %w", err)
		return nil, err
	}

	winner, err = gen.IsRoundWinner(ctx, base.TipSet, round, m.address, rbase, mbi, m.api)
	if err != nil {
		err = xerrors.Errorf("failed to check if we win next round: %w", err)
		return nil, err
	}

	if winner == nil {
		return nil, nil
	}

	tTicket := build.Clock.Now()

	buf := new(bytes.Buffer)
	if err := m.address.MarshalCBOR(buf); err != nil {
		err = xerrors.Errorf("failed to marshal miner address: %w", err)
		return nil, err
	}

	rand, err := lrand.DrawRandomness(rbase.Data, crypto.DomainSeparationTag_WinningPoStChallengeSeed, round, buf.Bytes())
	if err != nil {
		err = xerrors.Errorf("failed to get randomness for winning post: %w", err)
		return nil, err
	}

	prand := abi.PoStRandomness(rand)

	tSeed := build.Clock.Now()
	nv, err := m.api.StateNetworkVersion(ctx, base.TipSet.Key())
	if err != nil {
		return nil, err
	}

	postProof, err := m.epp.ComputeProof(ctx, mbi.Sectors, prand, round, nv)
	if err != nil {
		err = xerrors.Errorf("failed to compute winning post proof: %w", err)
		return nil, err
	}

	tProof := build.Clock.Now()

	// get pending messages early,
	msgs, err := m.api.MpoolSelect(context.TODO(), base.TipSet.Key(), ticket.Quality())
	if err != nil {
		err = xerrors.Errorf("failed to select messages for block: %w", err)
		return nil, err
	}

	tPending := build.Clock.Now()

	// TODO: winning post proof
	minedBlock, err = m.createBlock(base, m.address, ticket, winner, bvals, postProof, msgs)
	if err != nil {
		err = xerrors.Errorf("failed to create block: %w", err)
		return nil, err
	}

	tCreateBlock := build.Clock.Now()
	dur := tCreateBlock.Sub(tStart)
	parentMiners := make([]address.Address, len(base.TipSet.Blocks()))
	for i, header := range base.TipSet.Blocks() {
		parentMiners[i] = header.Miner
	}
	log.Infow("mined new block", "cid", minedBlock.Cid(), "height", int64(minedBlock.Header.Height), "miner", minedBlock.Header.Miner, "parents", parentMiners, "parentTipset", base.TipSet.Key().String(), "took", dur)
	if dur > time.Second*time.Duration(build.BlockDelaySecs) {
		log.Warnw("CAUTION: block production took longer than the block delay. Your computer may not be fast enough to keep up",
			"tPowercheck ", tPowercheck.Sub(tStart),
			"tTicket ", tTicket.Sub(tPowercheck),
			"tSeed ", tSeed.Sub(tTicket),
			"tProof ", tProof.Sub(tSeed),
			"tPending ", tPending.Sub(tProof),
			"tCreateBlock ", tCreateBlock.Sub(tPending))
	}

	return minedBlock, nil
}

func (m *Miner) mineOneOfSxx(ctx context.Context, base *MiningBase, addr string) (minedBlock *types.BlockMsg, err error) {
	log.Debugw("attempting to mine a block", "tipset", types.LogCids(base.TipSet.Cids()))
	tStart := build.Clock.Now()

	round := base.TipSet.Height() + base.NullRounds + 1

	// always write out a log
	var winner *types.ElectionProof
	var mbi *api.MiningBaseInfo
	var rbase types.BeaconEntry
	defer func() {

		var hasMinPower bool

		// mbi can be nil if we are deep in penalty and there are 0 eligible sectors
		// in the current deadline. If this case - put together a dummy one for reporting
		// https://github.com/filecoin-project/lotus/blob/v1.9.0/chain/stmgr/utils.go#L500-L502
		if mbi == nil {
			mbi = &api.MiningBaseInfo{
				NetworkPower:      big.NewInt(-1), // we do not know how big the network is at this point
				EligibleForMining: false,
				MinerPower:        big.NewInt(0), // but we do know we do not have anything eligible
			}

			// try to opportunistically pull actual power and plug it into the fake mbi
			// if pow, err := m.api.StateMinerPower(ctx, m.address, base.TipSet.Key()); err == nil && pow != nil {
			if pow, err := m.GetStateMinerPowerOfSxx(ctx, m.address, base.TipSet.Key(), addr); err == nil && pow != nil {
				hasMinPower = pow.HasMinPower
				mbi.MinerPower = pow.MinerPower.QualityAdjPower
				mbi.NetworkPower = pow.TotalPower.QualityAdjPower
			}
		}

		isLate := uint64(tStart.Unix()) > (base.TipSet.MinTimestamp() + uint64(base.NullRounds*builtin.EpochDurationSeconds) + build.PropagationDelaySecs)

		logStruct := []interface{}{
			"tookMilliseconds", (build.Clock.Now().UnixNano() - tStart.UnixNano()) / 1_000_000,
			"forRound", int64(round),
			"baseEpoch", int64(base.TipSet.Height()),
			"baseDeltaSeconds", uint64(tStart.Unix()) - base.TipSet.MinTimestamp(),
			"nullRounds", int64(base.NullRounds),
			"lateStart", isLate,
			"beaconEpoch", rbase.Round,
			"lookbackEpochs", int64(policy.ChainFinality), // hardcoded as it is unlikely to change again: https://github.com/filecoin-project/lotus/blob/v1.8.0/chain/actors/policy/policy.go#L180-L186
			"networkPowerAtLookback", mbi.NetworkPower.String(),
			"minerPowerAtLookback", mbi.MinerPower.String(),
			"isEligible", mbi.EligibleForMining,
			"isWinner", (winner != nil),
			"error", err,
		}

		if err != nil {
			log.Errorw("completed mineOne", logStruct...)
		} else if isLate || (hasMinPower && !mbi.EligibleForMining) {
			log.Warnw("completed mineOne", logStruct...)
		} else {
			log.Infow("completed mineOne", logStruct...)
		}
	}()

	// mbi, err = m.api.MinerGetBaseInfo(ctx, m.address, round, base.TipSet.Key())
	mbi, err = m.GetMinerGetBaseInfoOfSxx(ctx, m.address, round, base.TipSet.Key(), addr)
	if err != nil {
		err = xerrors.Errorf("failed to get mining base info: %w", err)
		return nil, err
	}
	if mbi == nil {
		return nil, nil
	}

	if !mbi.EligibleForMining {
		// slashed or just have no power yet
		return nil, nil
	}

	tPowercheck := build.Clock.Now()

	bvals := mbi.BeaconEntries
	rbase = mbi.PrevBeaconEntry
	if len(bvals) > 0 {
		rbase = bvals[len(bvals)-1]
	}

	ticket, err := m.computeTicket(ctx, &rbase, base, mbi)
	if err != nil {
		err = xerrors.Errorf("scratching ticket failed: %w", err)
		return nil, err
	}

	winner, err = gen.IsRoundWinner(ctx, base.TipSet, round, m.address, rbase, mbi, m.api)
	if err != nil {
		err = xerrors.Errorf("failed to check if we win next round: %w", err)
		return nil, err
	}

	if winner == nil {
		return nil, nil
	}

	tTicket := build.Clock.Now()

	buf := new(bytes.Buffer)
	if err := m.address.MarshalCBOR(buf); err != nil {
		err = xerrors.Errorf("failed to marshal miner address: %w", err)
		return nil, err
	}

	rand, err := lrand.DrawRandomness(rbase.Data, crypto.DomainSeparationTag_WinningPoStChallengeSeed, round, buf.Bytes())
	if err != nil {
		err = xerrors.Errorf("failed to get randomness for winning post: %w", err)
		return nil, err
	}

	prand := abi.PoStRandomness(rand)

	tSeed := build.Clock.Now()
	// nv, err := m.api.StateNetworkVersion(ctx, base.TipSet.Key())
	nv, err := m.GetStateNetworkVersionOfSxx(ctx, base.TipSet.Key(), addr)
	if err != nil {
		return nil, err
	}

	postProof, err := m.epp.ComputeProof(ctx, mbi.Sectors, prand, round, nv)
	if err != nil {
		err = xerrors.Errorf("failed to compute winning post proof: %w", err)
		return nil, err
	}

	tProof := build.Clock.Now()

	// get pending messages early,
	// msgs, err := m.api.MpoolSelect(context.TODO(), base.TipSet.Key(), ticket.Quality())
	msgs, err := m.GetMpoolSelectOfSxx(context.TODO(), base.TipSet.Key(), ticket.Quality(), addr)
	if err != nil {
		err = xerrors.Errorf("failed to select messages for block: %w", err)
		return nil, err
	}

	tPending := build.Clock.Now()

	// TODO: winning post proof
	// minedBlock, err = m.createBlock(base, m.address, ticket, winner, bvals, postProof, msgs)
	minedBlock, err = m.createBlockOfSxx(base, m.address, ticket, winner, bvals, postProof, msgs, addr)
	if err != nil {
		err = xerrors.Errorf("failed to create block: %w", err)
		return nil, err
	}

	tCreateBlock := build.Clock.Now()
	dur := tCreateBlock.Sub(tStart)
	parentMiners := make([]address.Address, len(base.TipSet.Blocks()))
	for i, header := range base.TipSet.Blocks() {
		parentMiners[i] = header.Miner
	}
	log.Infow("mined new block", "cid", minedBlock.Cid(), "height", int64(minedBlock.Header.Height), "miner", minedBlock.Header.Miner, "parents", parentMiners, "parentTipset", base.TipSet.Key().String(), "took", dur)
	if dur > time.Second*time.Duration(build.BlockDelaySecs) {
		log.Warnw("CAUTION: block production took longer than the block delay. Your computer may not be fast enough to keep up",
			"tPowercheck ", tPowercheck.Sub(tStart),
			"tTicket ", tTicket.Sub(tPowercheck),
			"tSeed ", tSeed.Sub(tTicket),
			"tProof ", tProof.Sub(tSeed),
			"tPending ", tPending.Sub(tProof),
			"tCreateBlock ", tCreateBlock.Sub(tPending))
	}

	return minedBlock, nil
}

func (m *Miner) computeTicket(ctx context.Context, brand *types.BeaconEntry, base *MiningBase, mbi *api.MiningBaseInfo) (*types.Ticket, error) {
	buf := new(bytes.Buffer)
	if err := m.address.MarshalCBOR(buf); err != nil {
		return nil, xerrors.Errorf("failed to marshal address to cbor: %w", err)
	}

	round := base.TipSet.Height() + base.NullRounds + 1
	if round > build.UpgradeSmokeHeight {
		buf.Write(base.TipSet.MinTicket().VRFProof)
	}

	input, err := lrand.DrawRandomness(brand.Data, crypto.DomainSeparationTag_TicketProduction, round-build.TicketRandomnessLookback, buf.Bytes())
	if err != nil {
		return nil, err
	}

	vrfOut, err := gen.ComputeVRF(ctx, m.api.WalletSign, mbi.WorkerKey, input)
	if err != nil {
		return nil, err
	}

	return &types.Ticket{
		VRFProof: vrfOut,
	}, nil
}

func (m *Miner) createBlock(base *MiningBase, addr address.Address, ticket *types.Ticket,
	eproof *types.ElectionProof, bvals []types.BeaconEntry, wpostProof []proof.PoStProof, msgs []*types.SignedMessage) (*types.BlockMsg, error) {
	uts := base.TipSet.MinTimestamp() + build.BlockDelaySecs*(uint64(base.NullRounds)+1)

	nheight := base.TipSet.Height() + base.NullRounds + 1

	// why even return this? that api call could just submit it for us
	return m.api.MinerCreateBlock(context.TODO(), &api.BlockTemplate{
		Miner:            addr,
		Parents:          base.TipSet.Key(),
		Ticket:           ticket,
		Eproof:           eproof,
		BeaconValues:     bvals,
		Messages:         msgs,
		Epoch:            nheight,
		Timestamp:        uts,
		WinningPoStProof: wpostProof,
	})
}

func (m *Miner) createBlockOfSxx(base *MiningBase, addr address.Address, ticket *types.Ticket,
	eproof *types.ElectionProof, bvals []types.BeaconEntry, wpostProof []proof.PoStProof, msgs []*types.SignedMessage, a string) (*types.BlockMsg, error) {
	uts := base.TipSet.MinTimestamp() + build.BlockDelaySecs*(uint64(base.NullRounds)+1)

	nheight := base.TipSet.Height() + base.NullRounds + 1

	// why even return this? that api call could just submit it for us
	// return m.api.MinerCreateBlock(context.TODO(), &api.BlockTemplate{
	// 	Miner:            addr,
	// 	Parents:          base.TipSet.Key(),
	// 	Ticket:           ticket,
	// 	Eproof:           eproof,
	// 	BeaconValues:     bvals,
	// 	Messages:         msgs,
	// 	Epoch:            nheight,
	// 	Timestamp:        uts,
	// 	WinningPoStProof: wpostProof,
	// })
	return m.GetMinerCreateBlockOfSxx(context.TODO(), &api.BlockTemplate{
		Miner:            addr,
		Parents:          base.TipSet.Key(),
		Ticket:           ticket,
		Eproof:           eproof,
		BeaconValues:     bvals,
		Messages:         msgs,
		Epoch:            nheight,
		Timestamp:        uts,
		WinningPoStProof: wpostProof,
	}, a)
}

// add by pan
func GetBestTipSet(ctx context.Context) *types.TipSet {
	addrs := []string{
		"https://rpc.ankr.com/filecoin",
		"https://api.node.glif.io",
	}
	var best *types.TipSet
	wg := sync.WaitGroup{}
	mutex := sync.Mutex{}
	for _, addr := range addrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			ts, err := GetTipSet(ctx, addr)
			if err != nil {
				return
			}
			mutex.Lock()
			if best == nil || best.Height() < ts.Height() {
				best = ts
			}
			mutex.Unlock()
		}(addr)
	}
	wg.Wait()
	return best
}

func GetTipSet(parent context.Context, addr string) (*types.TipSet, error) {
	ctx, cancelFunc := context.WithCancel(parent)
	go func() {
		time.Sleep(3 * time.Second)
		cancelFunc()
	}()
	var api api.FullNodeStruct
	closer, err := jsonrpc.NewMergeClient(ctx, addr, "Filecoin", []interface{}{&api.Internal, &api.CommonStruct.Internal}, nil)
	if err != nil {
		return nil, err
	}
	defer closer()
	return api.ChainHead(ctx)
}

// end
