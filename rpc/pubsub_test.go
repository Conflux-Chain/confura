package rpc

import (
	"encoding/json"
	"testing"

	"github.com/Conflux-Chain/confura/util"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/openweb3/go-rpc-provider"
	"github.com/stretchr/testify/assert"
)

func TestDelegateContextRegister(t *testing.T) {
	dctx := newDelegateContext()

	// test register
	rpcSubID := rpc.NewID()
	dsub, _ := dctx.registerDelegateSub(nil, rpcSubID, make(chan interface{}))
	assert.NotNil(t, dsub)

	tsub, ok := dctx.delegateSubs.Load(rpcSubID)
	assert.True(t, ok)

	dsub2, ok := tsub.(*delegateSubscription)
	assert.True(t, ok)
	assert.Equal(t, dsub, dsub2)

	// test deregister
	tsub = dctx.deregisterDelegateSub(rpcSubID)
	assert.NotNil(t, tsub)
	assert.Equal(t, dsub, tsub)

	numDelSubs := 0
	dctx.delegateSubs.Range(func(key, value interface{}) bool {
		numDelSubs++
		return true
	})
	assert.Equal(t, 0, numDelSubs)
}

func TestMatchLogFilterAddr(t *testing.T) {
	var (
		logFilter1 = &types.LogFilter{
			Address: []types.Address{},
		}
		logFilter2 = &types.LogFilter{
			Address: []types.Address{
				cfxaddress.MustNewFromBase32("cfx:acckucyy5fhzknbxmeexwtaj3bxmeg25b2b50pta6v"),
				cfxaddress.MustNewFromBase32("cfx:acdrf821t59y12b4guyzckyuw2xf1gfpj2ba0x4sj6"),
			},
		}
	)

	var (
		logJson1 = `{"address":"cfx:acg158kvr8zanb1bs048ryb6rtrhr283ma70vz70tx"}`
		logJson2 = `{"address":"cfx:acckucyy5fhzknbxmeexwtaj3bxmeg25b2b50pta6v"}`
		logJson3 = `{"address":"cfx:acdrf821t59y12b4guyzckyuw2xf1gfpj2ba0x4sj6"}`
		logJson4 = `{"address":""}`
	)

	testCases := []struct {
		logFilter   *types.LogFilter
		logJsonStr  string
		expectMatch bool
	}{
		// empty log filter
		{logFilter1, logJson1, true},
		{logFilter1, logJson4, true},

		// log address not contained in log filter
		{logFilter2, logJson1, false},
		{logFilter2, logJson4, false},
		// log address contained in log filter
		{logFilter2, logJson2, true},
		{logFilter2, logJson3, true},
	}

	for _, tc := range testCases {
		log := types.SubscriptionLog{}
		err := json.Unmarshal([]byte(tc.logJsonStr), &log)
		assert.Nil(t, err)

		assert.Equal(t, tc.expectMatch, util.IncludeCfxLogAddrs(log.Log, tc.logFilter.Address))
	}
}

func TestMatchLogFilterTopic(t *testing.T) {
	var (
		logFilter1 = &types.LogFilter{
			Topics: [][]types.Hash{},
		}
		logFilter2 = &types.LogFilter{
			Topics: [][]types.Hash{
				{
					types.Hash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"),
					types.Hash("0x2fe5be0146f74c5bce36c0b80911af6c7d86ff27e89d5cfa61fc681327954e5d"),
				},
			},
		}
		logFilter3 = &types.LogFilter{
			Topics: [][]types.Hash{
				{
					types.Hash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"),
					types.Hash("0x2fe5be0146f74c5bce36c0b80911af6c7d86ff27e89d5cfa61fc681327954e5d"),
				},
				{
					types.Hash("0x0000000000000000000000001e61c5dab363c1fdb903b61178b380d2cc7df999"),
					types.Hash("0x00000000000000000000000080ae6a88ce3351e9f729e8199f2871ba786ad7c5"),
				},
			},
		}
		logFilter4 = &types.LogFilter{
			Topics: [][]types.Hash{
				{
					types.Hash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"),
					types.Hash("0x2fe5be0146f74c5bce36c0b80911af6c7d86ff27e89d5cfa61fc681327954e5d"),
				},
				{
					types.Hash("0x0000000000000000000000001e61c5dab363c1fdb903b61178b380d2cc7df999"),
					types.Hash("0x00000000000000000000000080ae6a88ce3351e9f729e8199f2871ba786ad7c5"),
				},
				{
					types.Hash("0x0000000000000000000000008d545118d91c027c805c552f63a5c00a20ae6aca"),
					types.Hash("0x0000000000000000000000001a7fabd17788269d52ef0850f2e0dcbf444a9403"),
				},
			},
		}
		logFilter5 = &types.LogFilter{
			Topics: [][]types.Hash{
				{
					types.Hash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"),
					types.Hash("0x2fe5be0146f74c5bce36c0b80911af6c7d86ff27e89d5cfa61fc681327954e5d"),
				},
				{
					types.Hash("0x0000000000000000000000001e61c5dab363c1fdb903b61178b380d2cc7df999"),
					types.Hash("0x00000000000000000000000080ae6a88ce3351e9f729e8199f2871ba786ad7c5"),
				},
				{
					types.Hash("0x0000000000000000000000008d545118d91c027c805c552f63a5c00a20ae6aca"),
					types.Hash("0x0000000000000000000000001a7fabd17788269d52ef0850f2e0dcbf444a9403"),
				},
				{
					types.Hash("0x00000000000000000000000019f4bcf113e0b896d9b34294fd3da86b4adf0302"),
					types.Hash("0x000000000000000000000000160ebef20c1f739957bf9eecd040bce699cc42c6"),
				},
			},
		}
	)

	var (
		logJson1 = `{"topics":[]}`
		logJson2 = `{"topics":["0x2fe5be0146f74c5bce36c0b80911af6c7d86ff27e89d5cfa61fc681327954e5d"]}`
		logJson3 = `{"topics":["0x2fe5be0146f74c5bce36c0b80911af6c7d86ff27e89d5cfa61fc681327954e5d","0x00000000000000000000000080ae6a88ce3351e9f729e8199f2871ba786ad7c5"]}`
		logJson4 = `{"topics":["0x2fe5be0146f74c5bce36c0b80911af6c7d86ff27e89d5cfa61fc681327954e5d","0x00000000000000000000000080ae6a88ce3351e9f729e8199f2871ba786ad7c5","0x0000000000000000000000001a7fabd17788269d52ef0850f2e0dcbf444a9403"]}`
		logJson5 = `{"topics":["0x2fe5be0146f74c5bce36c0b80911af6c7d86ff27e89d5cfa61fc681327954e5d","0x00000000000000000000000080ae6a88ce3351e9f729e8199f2871ba786ad7c5","0x0000000000000000000000001a7fabd17788269d52ef0850f2e0dcbf444a9403", "0x00000000000000000000000019f4bcf113e0b896d9b34294fd3da86b4adf0302"]}`
		logJson6 = `{"topics":["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef","0x00000000000000000000000019f4bcf113e0b896d9b34294fd3da86b4adf0302","0x0000000000000000000000008d545118d91c027c805c552f63a5c00a20ae6aca", "0x2fe5be0146f74c5bce36c0b80911af6c7d86ff27e89d5cfa61fc681327954e5d"]}`
	)

	testCases := []struct {
		logFilter   *types.LogFilter
		logJsonStr  string
		expectMatch bool
	}{
		// empty log filter
		{logFilter1, logJson1, true},
		{logFilter1, logJson2, true},
		{logFilter1, logJson3, true},
		{logFilter1, logJson4, true},
		{logFilter1, logJson5, true},
		{logFilter1, logJson6, true},
		// one topic filter
		{logFilter2, logJson1, false},
		{logFilter2, logJson2, true},
		{logFilter2, logJson3, true},
		{logFilter2, logJson4, true},
		{logFilter2, logJson5, true},
		{logFilter2, logJson6, true},
		// two topics filter
		{logFilter3, logJson1, false},
		{logFilter3, logJson2, false},
		{logFilter3, logJson3, true},
		{logFilter3, logJson4, true},
		{logFilter3, logJson5, true},
		{logFilter3, logJson6, false},
		// three topics filter
		{logFilter4, logJson1, false},
		{logFilter4, logJson2, false},
		{logFilter4, logJson3, false},
		{logFilter4, logJson4, true},
		{logFilter4, logJson5, true},
		{logFilter4, logJson6, false},
		// four topics filter
		{logFilter5, logJson1, false},
		{logFilter5, logJson2, false},
		{logFilter5, logJson3, false},
		{logFilter5, logJson4, false},
		{logFilter5, logJson5, true},
		{logFilter5, logJson6, false},
	}

	for _, tc := range testCases {
		log := types.SubscriptionLog{}
		err := json.Unmarshal([]byte(tc.logJsonStr), &log)
		assert.Nil(t, err)
		assert.Equal(t, tc.expectMatch, util.MatchCfxLogTopics(log.Log, tc.logFilter.Topics))
	}
}

func TestMatchPubSubLogFilter(t *testing.T) {
	var (
		logFilter1 = &types.LogFilter{
			Address: []types.Address{},
			Topics:  [][]types.Hash{},
		}
		logFilter2 = &types.LogFilter{
			Address: []types.Address{cfxaddress.MustNewFromBase32("cfx:acdrf821t59y12b4guyzckyuw2xf1gfpj2ba0x4sj6")},
			Topics: [][]types.Hash{
				{types.Hash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")},
				{types.Hash("0x0000000000000000000000001e61c5dab363c1fdb903b61178b380d2cc7df999")},
				{types.Hash("0x0000000000000000000000008d545118d91c027c805c552f63a5c00a20ae6aca")},
				{types.Hash("0x00000000000000000000000019f4bcf113e0b896d9b34294fd3da86b4adf0302")},
			},
		}
	)

	var (
		logJson1 = `{"revertTo":"0x40f"}` // reorg log shall always be matched
		logJson2 = `{"address":"","topics":[]}`
		logJson3 = `{"address":"cfx:acdrf821t59y12b4guyzckyuw2xf1gfpj2ba0x4sj6","topics":["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef","0x0000000000000000000000001e61c5dab363c1fdb903b61178b380d2cc7df999","0x0000000000000000000000008d545118d91c027c805c552f63a5c00a20ae6aca", "0x00000000000000000000000019f4bcf113e0b896d9b34294fd3da86b4adf0302"]}`
		logJson4 = `{"address":"cfx:acckucyy5fhzknbxmeexwtaj3bxmeg25b2b50pta6v","topics":["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef","0x0000000000000000000000001e61c5dab363c1fdb903b61178b380d2cc7df999","0x0000000000000000000000008d545118d91c027c805c552f63a5c00a20ae6aca", "0x00000000000000000000000019f4bcf113e0b896d9b34294fd3da86b4adf0302"]}`
		logJson5 = `{"address":"cfx:acdrf821t59y12b4guyzckyuw2xf1gfpj2ba0x4sj6","topics":["0x2fe5be0146f74c5bce36c0b80911af6c7d86ff27e89d5cfa61fc681327954e5d","0x00000000000000000000000080ae6a88ce3351e9f729e8199f2871ba786ad7c5","0x0000000000000000000000001a7fabd17788269d52ef0850f2e0dcbf444a9403", "0x000000000000000000000000160ebef20c1f739957bf9eecd040bce699cc42c6"]}`
	)

	testCases := []struct {
		logFilter   *types.LogFilter
		logJsonStr  string
		expectMatch bool
	}{
		// empty log filter
		{logFilter1, logJson1, true},
		{logFilter1, logJson2, true},

		{logFilter2, logJson1, true},
		{logFilter2, logJson2, false},
		{logFilter2, logJson3, true},
		{logFilter2, logJson4, false},
		{logFilter2, logJson5, false},
	}

	for _, tc := range testCases {
		log := types.SubscriptionLog{}
		err := json.Unmarshal([]byte(tc.logJsonStr), &log)
		assert.Nil(t, err)

		assert.Equal(t, tc.expectMatch, matchPubSubLogFilter(&log, tc.logFilter))
	}
}
