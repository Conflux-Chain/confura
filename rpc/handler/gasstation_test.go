package handler

import (
	"math/big"
	"testing"

	"github.com/Conflux-Chain/confura/types"
	"github.com/stretchr/testify/assert"
)

func TestBlockPriorityFee(t *testing.T) {
	blockFee := &BlockPriorityFee{}
	blockFee.Append([]*TxnPriorityFee{
		{tip: big.NewInt(10000000000)},
		{tip: big.NewInt(5000000000)},
		{tip: big.NewInt(1000000000)},
		{tip: big.NewInt(9000000000)},
		{tip: big.NewInt(3000000000)},
		{tip: big.NewInt(4000000000)},
		{tip: big.NewInt(7000000000)},
		{tip: big.NewInt(6000000000)},
		{tip: big.NewInt(8000000000)},
		{tip: big.NewInt(2000000000)},
	}...)

	for i := 0; i < len(blockFee.txnTips)-1; i++ {
		cmp := blockFee.txnTips[i].tip.Cmp(blockFee.txnTips[i+1].tip)
		assert.True(t, cmp <= 0, "block txn tip fees should be sorted in ascending order")
	}

	tipRange := blockFee.TipRange()
	assert.Equal(t, big.NewInt(1000000000), tipRange[0], "min tip should be 1000000000")
	assert.Equal(t, big.NewInt(10000000000), tipRange[1], "max tip should be 10000000000")

	p0Fee := blockFee.Percentile(0)
	assert.Equal(t, big.NewInt(1000000000), p0Fee, "0th percentile tip should be 1000000000")

	p10Fee := blockFee.Percentile(10)
	assert.Equal(t, big.NewInt(1000000000), p10Fee, "10th percentile tip should be 1000000000")

	p11Fee := blockFee.Percentile(11)
	assert.Equal(t, big.NewInt(2000000000), p11Fee, "11th percentile tip should be 2000000000")

	p20Fee := blockFee.Percentile(20)
	assert.Equal(t, big.NewInt(2000000000), p20Fee, "20th percentile tip should be 2000000000")

	p30Fee := blockFee.Percentile(30)
	assert.Equal(t, big.NewInt(3000000000), p30Fee, "30th percentile tip should be 3000000000")

	p40Fee := blockFee.Percentile(40)
	assert.Equal(t, big.NewInt(4000000000), p40Fee, "40th percentile tip should be 4000000000")

	p50Fee := blockFee.Percentile(50)
	assert.Equal(t, big.NewInt(5000000000), p50Fee, "50th percentile tip should be 5000000000")

	p60Fee := blockFee.Percentile(60)
	assert.Equal(t, big.NewInt(6000000000), p60Fee, "60th percentile tip should be 6000000000")

	p70Fee := blockFee.Percentile(70)
	assert.Equal(t, big.NewInt(7000000000), p70Fee, "70th percentile tip should be 7000000000")

	p80Fee := blockFee.Percentile(80)
	assert.Equal(t, big.NewInt(8000000000), p80Fee, "80th percentile tip should be 8000000000")

	p90Fee := blockFee.Percentile(90)
	assert.Equal(t, big.NewInt(9000000000), p90Fee, "90th percentile tip should be 9000000000")

	p100Fee := blockFee.Percentile(100)
	assert.Equal(t, big.NewInt(10000000000), p100Fee, "100th percentile tip should be 10000000000")
}

func TestPriorityFeeWindow(t *testing.T) {
	feeWin := NewPriorityFeeWindow(2)

	// push block 1
	testBlockFee := &BlockPriorityFee{
		number:       1,
		hash:         "0xee373c1b44c6383fbcbb6ad2e9a1961fe0947b0f4624bde6af4f0ca64f76e001",
		baseFee:      big.NewInt(1000000000),
		gasUsedRatio: 0.1,
	}
	testBlockFee.Append([]*TxnPriorityFee{
		{tip: big.NewInt(5000000000)},
		{tip: big.NewInt(1000000000)},
		{tip: big.NewInt(3000000000)},
		{tip: big.NewInt(4000000000)},
		{tip: big.NewInt(2000000000)},
	}...)
	feeWin.Push(testBlockFee)

	// push block 2
	testBlockFee = &BlockPriorityFee{
		number:       2,
		hash:         "0xee373c1b44c6383fbcbb6ad2e9a1961fe0947b0f4624bde6af4f0ca64f76e002",
		baseFee:      big.NewInt(2100000000),
		gasUsedRatio: 0.2,
	}
	testBlockFee.Append([]*TxnPriorityFee{
		{tip: big.NewInt(6000000000)},
		{tip: big.NewInt(7000000000)},
		{tip: big.NewInt(9000000000)},
		{tip: big.NewInt(8000000000)},
		{tip: big.NewInt(10000000000)},
	}...)
	feeWin.Push(testBlockFee)

	// validation 1
	assert.Equal(t, 2, feeWin.Size(), "window size should be 2")

	assert.True(
		t, feeWin.historicalBaseFeeRanges[0].Cmp(big.NewInt(1000000000)) == 0 &&
			feeWin.historicalBaseFeeRanges[1].Cmp(big.NewInt(2100000000)) == 0,
		"historical base fee should be within the range (1000000000,2100000000)",
	)
	assert.True(
		t, feeWin.historicalPriorityFeeRange[0].Cmp(big.NewInt(1000000000)) == 0 &&
			feeWin.historicalPriorityFeeRange[1].Cmp(big.NewInt(10000000000)) == 0,
		"historical base fee should be within the range (1000000000,10000000000)",
	)

	latestPriorityFeeRange := feeWin.calculateLatestPriorityFeeRange()
	assert.True(
		t, feeWin.historicalPriorityFeeRange[0].Cmp(latestPriorityFeeRange[0]) == 0 &&
			feeWin.historicalPriorityFeeRange[1].Cmp(latestPriorityFeeRange[1]) == 0,
		"historical base fee should be the same range with latest priority fee range",
	)

	assert.Equal(t, 0.2, feeWin.calculateNetworkCongestion(), "network congestion should be 0.2")

	estPriorityFees := feeWin.calculateAvgPriorityFees([]float64{1, 50, 99})
	assert.Equal(
		t, big.NewInt(3500000000), estPriorityFees[0], "1st percentile priority fee should be 3500000000",
	)
	assert.Equal(
		t, big.NewInt(5500000000), estPriorityFees[1], "50th percentile priority fee should be 5500000000",
	)
	assert.Equal(
		t, big.NewInt(7500000000), estPriorityFees[2], "99th percentile priority fee should be 7500000000",
	)

	priorityFeeTrend, baseFeeTrend := feeWin.calculateFeeTrend()
	assert.Equal(t, types.GasFeeTrendUp, priorityFeeTrend, "priority fee trend should be up")
	assert.Equal(t, types.GasFeeTrendUp, baseFeeTrend, "base fee trend should be up")

	// push block 3
	testBlockFee = &BlockPriorityFee{
		number:       3,
		hash:         "0xee373c1b44c6383fbcbb6ad2e9a1961fe0947b0f4624bde6af4f0ca64f76e003",
		baseFee:      big.NewInt(3500000000),
		gasUsedRatio: 0.3,
	}
	testBlockFee.Append([]*TxnPriorityFee{
		{tip: big.NewInt(15000000000)},
		{tip: big.NewInt(11000000000)},
		{tip: big.NewInt(13000000000)},
		{tip: big.NewInt(12000000000)},
		{tip: big.NewInt(14000000000)},
	}...)
	feeWin.Push(testBlockFee)

	// validation 2
	assert.Equal(t, 2, feeWin.Size(), "window size should be 2")

	assert.True(
		t, feeWin.historicalBaseFeeRanges[0].Cmp(big.NewInt(1000000000)) == 0 &&
			feeWin.historicalBaseFeeRanges[1].Cmp(big.NewInt(3500000000)) == 0,
		"historical base fee should be within the range (1000000000,3500000000)",
	)
	assert.True(
		t, feeWin.historicalPriorityFeeRange[0].Cmp(big.NewInt(1000000000)) == 0 &&
			feeWin.historicalPriorityFeeRange[1].Cmp(big.NewInt(15000000000)) == 0,
		"historical base fee should be within the range (1000000000,15000000000)",
	)

	latestPriorityFeeRange = feeWin.calculateLatestPriorityFeeRange()
	assert.True(
		t, latestPriorityFeeRange[0].Cmp(big.NewInt(6000000000)) == 0 &&
			latestPriorityFeeRange[1].Cmp(big.NewInt(15000000000)) == 0,
		"historical base fee should be within the range (6000000000,15000000000)",
	)

	assert.Equal(t, 0.3, feeWin.calculateNetworkCongestion(), "network congestion should be 0.3")

	estPriorityFees = feeWin.calculateAvgPriorityFees([]float64{1, 50, 99})
	assert.Equal(
		t, big.NewInt(8500000000), estPriorityFees[0], "1st percentile priority fee should be 8500000000",
	)
	assert.Equal(
		t, big.NewInt(10500000000), estPriorityFees[1], "50th percentile priority fee should be 10500000000",
	)
	assert.Equal(
		t, big.NewInt(12500000000), estPriorityFees[2], "99th percentile priority fee should be 12500000000",
	)
}
