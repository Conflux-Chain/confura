package handler

import (
	"container/list"
	"math"
	"math/big"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/types"
	logutil "github.com/Conflux-Chain/go-conflux-util/log"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/openweb3/go-rpc-provider/utils"
	"github.com/sirupsen/logrus"
)

const (
	// The interval to sync data in normal mode.
	syncIntervalNormal time.Duration = time.Second
	// The interval to sync data in catch-up mode.
	syncIntervalCatchUp time.Duration = time.Millisecond
	// The interval to update the node cluster.
	clusterUpdateInterval = time.Minute
)

// GasStationStatus represents the status of gas station.
type GasStationStatus struct{ error }

var (
	StationStatusOk                GasStationStatus = GasStationStatus{}
	StationStatusClientUnavailable GasStationStatus = GasStationStatus{node.ErrClientUnavailable}
)

func newGasStationStatus(err error) GasStationStatus {
	if err != nil && !utils.IsRPCJSONError(err) {
		// Regarded as unavailable due to non JSON-RPC error eg., io error.
		return GasStationStatus{err}
	}

	return StationStatusOk
}

type GasStationConfig struct {
	// Whether to enable gas station.
	Enabled bool
	// Number of blocks/epochs to peek for gas price estimation.
	HistoricalPeekCount int `default:"100"`
	// Percentiles for average txn gas price mapped to three levels of urgency (`low`, `medium` and `high`).
	Percentiles [3]float64 `default:"[1, 50, 99]"`
}

type baseGasStationHandler struct {
	config        *GasStationConfig  // Gas station configuration
	status        atomic.Value       // Gas station status
	blockHashList *list.List         // Linked list to store historical block hashes for reorg detection
	window        *PriorityFeeWindow // Block priority fee window
}

func newBaseGasStationHandler(config *GasStationConfig) *baseGasStationHandler {
	return &baseGasStationHandler{
		config:        config,
		blockHashList: list.New(),
		window:        NewPriorityFeeWindow(config.HistoricalPeekCount),
	}
}

// run starts to sync historical data and refresh cluster nodes.
func (h *baseGasStationHandler) run(sync func() (bool, error), refresh func() error) {
	syncTicker := time.NewTimer(0)
	defer syncTicker.Stop()

	refreshTicker := time.NewTicker(clusterUpdateInterval)
	defer refreshTicker.Stop()

	etLogger := logutil.NewErrorTolerantLogger(logutil.DefaultETConfig)
	for {
		select {
		case <-syncTicker.C:
			complete, err := sync()
			h.status.Store(newGasStationStatus(err))
			h.resetSyncTicker(syncTicker, complete, err)
			etLogger.Log(logrus.StandardLogger(), err, "Gas Station handler sync error")
		case <-refreshTicker.C:
			if err := refresh(); err != nil {
				logrus.WithError(err).Error("Gas station handler cluster refresh error")
			}
		}
	}
}

func (h *baseGasStationHandler) resetSyncTicker(syncTicker *time.Timer, complete bool, err error) {
	switch {
	case err != nil:
		syncTicker.Reset(syncIntervalNormal)
	case complete:
		syncTicker.Reset(syncIntervalNormal)
	default:
		syncTicker.Reset(syncIntervalCatchUp)
	}
}

func (h *baseGasStationHandler) checkStatus() error {
	if status := h.status.Load(); status != nil && status != StationStatusOk {
		return status.(error)
	}

	return nil
}

// BlockPriorityFee holds the gas fees of transactions within a single block.
type BlockPriorityFee struct {
	number       uint64            // Block number
	hash         string            // Block hash
	baseFee      *big.Int          // Base fee per gas of the block
	gasUsedRatio float64           // Gas used ratio of the block
	txnTips      []*TxnPriorityFee // Slice of ordered transaction priority fees
}

// Append appends transaction priority fees to the block and sorts them in ascending order.
func (b *BlockPriorityFee) Append(txnTips ...*TxnPriorityFee) {
	b.txnTips = append(b.txnTips, txnTips...)
	slices.SortFunc(b.txnTips, func(l, r *TxnPriorityFee) int {
		return l.tip.Cmp(r.tip)
	})
}

// Percentile calculates the percentile of transaction priority fees
func (b *BlockPriorityFee) Percentile(p float64) *big.Int {
	// Return nil if there are no transaction tips
	if len(b.txnTips) == 0 {
		return nil
	}

	// Ensure the percentile is between 0 and 100
	if p < 0 || p > 100 {
		return nil
	}

	// Calculate the index corresponding to the given percentile
	index := 0
	if p > 0 {
		index = int(math.Ceil(p*float64(len(b.txnTips))/100)) - 1
	}

	// Ensure the index is within bounds
	if index >= len(b.txnTips) {
		index = len(b.txnTips) - 1
	}

	return b.txnTips[index].tip
}

// TipRange returns the range of transaction priority fees in the block.
func (b *BlockPriorityFee) TipRange() []*big.Int {
	if len(b.txnTips) == 0 {
		return nil
	}

	return []*big.Int{b.txnTips[0].tip, b.txnTips[len(b.txnTips)-1].tip}
}

// TxnPriorityFee holds the priority fee information of a single transaction.
type TxnPriorityFee struct {
	hash string   // Transaction hash
	tip  *big.Int // Priority fee of the transaction
}

// PriorityFeeWindow holds priority fees of the latest blocks using a sliding window mechanism.
type PriorityFeeWindow struct {
	mu                         sync.Mutex
	feeChain                   *list.List               // List of chronologically ordered blocks
	hashToFee                  map[string]*list.Element // Map of block hash to linked list element
	capacity                   int                      // Number of blocks to maintain in the window
	historicalBaseFeeRanges    []*big.Int               // Range of base fees per gas over a historical period
	historicalPriorityFeeRange []*big.Int               // Range of priority fees per gas over a historical period
}

// NewPriorityFeeWindow creates a new `PriorityFeeWindow` with the specified capacity.
func NewPriorityFeeWindow(capacity int) *PriorityFeeWindow {
	return &PriorityFeeWindow{
		feeChain:  list.New(),
		hashToFee: make(map[string]*list.Element),
		capacity:  capacity,
	}
}

// Size returns the number of blocks in the window.
func (w *PriorityFeeWindow) Size() int {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.feeChain.Len()
}

// Remove removes blocks from the window.
func (w *PriorityFeeWindow) Remove(blockHashes ...string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for i := range blockHashes {
		if e, ok := w.hashToFee[blockHashes[i]]; ok {
			w.feeChain.Remove(e)
			delete(w.hashToFee, blockHashes[i])
		}
	}
}

// Push adds a new block to the window.
func (w *PriorityFeeWindow) Push(blockFee *BlockPriorityFee) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, ok := w.hashToFee[blockFee.hash]; ok { // Block already exists?
		return
	}

	w.insertBlock(blockFee)
	w.updateHistoricalBaseFeeRange(blockFee)
	w.updateHistoricalPriorityFeeRange(blockFee)

	// If the window is full, prune the oldest block.
	for w.feeChain.Len() > w.capacity {
		e := w.feeChain.Front()
		blockFee := e.Value.(*BlockPriorityFee)

		delete(w.hashToFee, blockFee.hash)
		w.feeChain.Remove(e)
	}
}

// insertBlock inserts a block to the linked list.
func (w *PriorityFeeWindow) insertBlock(blockFee *BlockPriorityFee) {
	// Locate the postion where this block should be inserted
	var e *list.Element
	for e = w.feeChain.Back(); e != nil; e = e.Prev() {
		if e.Value.(*BlockPriorityFee).number < blockFee.number {
			break
		}
	}

	if e == nil {
		w.hashToFee[blockFee.hash] = w.feeChain.PushFront(blockFee)
	} else {
		w.hashToFee[blockFee.hash] = w.feeChain.InsertAfter(blockFee, e)
	}
}

func (w *PriorityFeeWindow) updateHistoricalBaseFeeRange(blockFee *BlockPriorityFee) {
	if blockFee.baseFee == nil {
		return
	}

	// Update the historical base fee range
	if w.historicalBaseFeeRanges == nil { // initial setup
		w.historicalBaseFeeRanges = []*big.Int{
			big.NewInt(0).Set(blockFee.baseFee), big.NewInt(0).Set(blockFee.baseFee),
		}
		return
	}

	if blockFee.baseFee.Cmp(w.historicalBaseFeeRanges[0]) < 0 { // update min
		w.historicalBaseFeeRanges[0].Set(blockFee.baseFee)
	}

	if blockFee.baseFee.Cmp(w.historicalBaseFeeRanges[1]) > 0 { // update max
		w.historicalBaseFeeRanges[1].Set(blockFee.baseFee)
	}
}

func (w *PriorityFeeWindow) updateHistoricalPriorityFeeRange(blockFee *BlockPriorityFee) {
	tipRange := blockFee.TipRange()
	if tipRange == nil {
		return
	}

	if w.historicalPriorityFeeRange == nil { // initial setup
		w.historicalPriorityFeeRange = []*big.Int{
			big.NewInt(0).Set(tipRange[0]), big.NewInt(0).Set(tipRange[1]),
		}
		return
	}

	if tipRange[0].Cmp(w.historicalPriorityFeeRange[0]) < 0 { // update min
		w.historicalPriorityFeeRange[0] = big.NewInt(0).Set(tipRange[0])
	}

	if tipRange[1].Cmp(w.historicalPriorityFeeRange[1]) > 0 { // update max
		w.historicalPriorityFeeRange[1] = big.NewInt(0).Set(tipRange[1])
	}
}

type GasStats struct {
	NetworkCongestion          float64           // Current congestion on the network (0 to 1)
	LatestPriorityFeeRange     []*big.Int        // Range of priority fees for recent transactions
	HistoricalPriorityFeeRange []*big.Int        // Range of priority fees over a historical period
	HistoricalBaseFeeRange     []*big.Int        // Range of base fees over a historical period
	AvgPercentiledPriorityFee  []*big.Int        // Average priority fee per gas by given percentiles
	PriorityFeeTrend           types.GasFeeTrend // Current trend in priority fees
	BaseFeeTrend               types.GasFeeTrend // Current trend in base fees
}

// Calculate calculates the gas fee statistics from the data within the window.
func (w *PriorityFeeWindow) Calculate(percentiles []float64) (stats GasStats) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Calculate the average priority fee of the given percentiles.
	stats.AvgPercentiledPriorityFee = w.calculateAvgPriorityFees(percentiles)

	// Calculate latest priority fee range.
	stats.LatestPriorityFeeRange = w.calculateLatestPriorityFeeRange()
	stats.HistoricalPriorityFeeRange = w.historicalPriorityFeeRange
	stats.HistoricalBaseFeeRange = w.historicalBaseFeeRanges

	// Calculate the network congestion.
	stats.NetworkCongestion = w.calculateNetworkCongestion()

	// Calculate the trend of priority fee and base fee.
	stats.PriorityFeeTrend, stats.BaseFeeTrend = w.calculateFeeTrend()

	return stats
}

func (w *PriorityFeeWindow) calculateAvgPriorityFees(percentiles []float64) (res []*big.Int) {
	for _, p := range percentiles {
		if fee := w.calculateAvgPriorityFee(p); fee == nil {
			return nil
		} else {
			res = append(res, fee)
		}
	}

	return res
}

func (w *PriorityFeeWindow) calculateAvgPriorityFee(p float64) *big.Int {
	totalFee := big.NewInt(0)
	totalSize := int64(0)

	for e := w.feeChain.Front(); e != nil; e = e.Next() {
		blockFee := e.Value.(*BlockPriorityFee)
		if v := blockFee.Percentile(p); v != nil {
			totalFee.Add(totalFee, v)
			totalSize++
		}
	}

	if totalSize != 0 { // Return the average priority fee per gas per block in the window.
		return totalFee.Div(totalFee, big.NewInt(totalSize))
	}

	return nil
}

func (w *PriorityFeeWindow) calculateFeeTrend() (priorityFeeTrend, baseFeeTrend types.GasFeeTrend) {
	if w.feeChain.Len() < 2 {
		return types.GasFeeTrendUp, types.GasFeeTrendUp
	}

	latestBlockFee := w.feeChain.Back().Value.(*BlockPriorityFee)
	prevBlockFee := w.feeChain.Back().Prev().Value.(*BlockPriorityFee)

	baseFeeTrend = w.determineTrend(prevBlockFee.baseFee, latestBlockFee.baseFee)
	priorityFeeTrend = w.determinePriorityFeeTrend(prevBlockFee, latestBlockFee)

	return priorityFeeTrend, baseFeeTrend
}

func (w *PriorityFeeWindow) determineTrend(prevFee, latestFee *big.Int) types.GasFeeTrend {
	if cmp := prevFee.Cmp(latestFee); cmp < 0 {
		return types.GasFeeTrendUp
	} else if cmp > 0 {
		return types.GasFeeTrendDown
	}

	return ""
}

func (w *PriorityFeeWindow) determinePriorityFeeTrend(prevBlockFee, latestBlockFee *BlockPriorityFee) types.GasFeeTrend {
	prevAvgP50 := prevBlockFee.Percentile(50)
	if prevAvgP50 == nil {
		prevAvgP50 = big.NewInt(0)
	}

	latestAvgP50 := latestBlockFee.Percentile(50)
	if latestAvgP50 == nil {
		latestAvgP50 = big.NewInt(0)
	}

	if cmp := prevAvgP50.Cmp(latestAvgP50); cmp < 0 {
		return types.GasFeeTrendUp
	} else if cmp > 0 {
		return types.GasFeeTrendDown
	}

	return ""
}

func (w *PriorityFeeWindow) calculateNetworkCongestion() float64 {
	if e := w.feeChain.Back(); e != nil {
		return e.Value.(*BlockPriorityFee).gasUsedRatio
	}

	return 0
}

func (w *PriorityFeeWindow) calculateLatestPriorityFeeRange() (res []*big.Int) {
	for e := w.feeChain.Front(); e != nil; e = e.Next() {
		tipRange := e.Value.(*BlockPriorityFee).TipRange()
		if tipRange == nil { // skip empty range
			continue
		}

		if res == nil { // initial setup
			res = []*big.Int{
				big.NewInt(0).Set(tipRange[0]), big.NewInt(0).Set(tipRange[1]),
			}
			continue
		}

		if tipRange[0].Cmp(res[0]) < 0 {
			res[0] = big.NewInt(0).Set(tipRange[0])
		}

		if tipRange[1].Cmp(res[1]) > 0 {
			res[1] = big.NewInt(0).Set(tipRange[1])
		}
	}

	return res
}

// ToHexBigSlice converts a slice of `*big.Int` to a slice of `*hexutil.Big`.
func ToHexBigSlice(arr []*big.Int) []*hexutil.Big {
	if len(arr) == 0 {
		return nil
	}

	res := make([]*hexutil.Big, len(arr))
	for i, v := range arr {
		res[i] = (*hexutil.Big)(v)
	}
	return res
}

func assembleSuggestedGasFees(baseFeePerGas *big.Int, stats *GasStats) *types.SuggestedGasFees {
	priorityFees := stats.AvgPercentiledPriorityFee
	return &types.SuggestedGasFees{
		Low: types.GasFeeEstimation{
			SuggestedMaxPriorityFeePerGas: (*hexutil.Big)(priorityFees[0]),
			SuggestedMaxFeePerGas:         (*hexutil.Big)(big.NewInt(0).Add(baseFeePerGas, priorityFees[0])),
		},
		Medium: types.GasFeeEstimation{
			SuggestedMaxPriorityFeePerGas: (*hexutil.Big)(priorityFees[1]),
			SuggestedMaxFeePerGas:         (*hexutil.Big)(big.NewInt(0).Add(baseFeePerGas, priorityFees[1])),
		},
		High: types.GasFeeEstimation{
			SuggestedMaxPriorityFeePerGas: (*hexutil.Big)(priorityFees[2]),
			SuggestedMaxFeePerGas:         (*hexutil.Big)(big.NewInt(0).Add(baseFeePerGas, priorityFees[2])),
		},
		EstimatedBaseFee:           (*hexutil.Big)(baseFeePerGas),
		NetworkCongestion:          stats.NetworkCongestion,
		LatestPriorityFeeRange:     ToHexBigSlice(stats.LatestPriorityFeeRange),
		HistoricalPriorityFeeRange: ToHexBigSlice(stats.HistoricalPriorityFeeRange),
		HistoricalBaseFeeRange:     ToHexBigSlice(stats.HistoricalBaseFeeRange),
		PriorityFeeTrend:           stats.PriorityFeeTrend,
		BaseFeeTrend:               stats.BaseFeeTrend,
	}
}
