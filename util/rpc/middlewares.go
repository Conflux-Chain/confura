package rpc

import (
	"fmt"
	"time"

	"github.com/conflux-chain/conflux-infura/util/metrics"
	"github.com/openweb3/go-rpc-provider/utils"
	"github.com/sirupsen/logrus"
)

func middlewareMetrics(
	fullnode, space string,
	handler func(result interface{}, method string, args ...interface{}) error,
) func(result interface{}, method string, args ...interface{}) error {
	return func(result interface{}, method string, args ...interface{}) error {
		start := time.Now()

		err := handler(result, method, args...)

		var metricKey string
		if err != nil {
			metricKey = fmt.Sprintf("infura/rpc/fullnode/%v/%v/failure", space, method)
		} else {
			metricKey = fmt.Sprintf("infura/rpc/fullnode/%v/%v/success", space, method)
		}

		metrics.GetOrRegisterTimer(metricKey).UpdateSince(start)

		// overall error rate for each full node
		metrics.GetOrRegisterTimeWindowPercentageDefault("infura/rpc/fullnode/rate/error").Mark(err != nil)
		metrics.GetOrRegisterTimeWindowPercentageDefault("infura/rpc/fullnode/rate/error/%v", fullnode).Mark(err != nil)
		nonRpcErr := err != nil && !utils.IsRPCJSONError(err) // generally io error
		metrics.GetOrRegisterTimeWindowPercentageDefault("infura/rpc/fullnode/rate/nonRpcErr").Mark(nonRpcErr)
		metrics.GetOrRegisterTimeWindowPercentageDefault("infura/rpc/fullnode/rate/nonRpcErr/%v", fullnode).Mark(nonRpcErr)

		return err
	}
}

func middlewareLog(
	fullnode, space string,
	handler func(result interface{}, method string, args ...interface{}) error,
) func(result interface{}, method string, args ...interface{}) error {
	return func(result interface{}, method string, args ...interface{}) error {
		if !logrus.IsLevelEnabled(logrus.DebugLevel) {
			return handler(result, method, args...)
		}

		logger := logrus.WithFields(logrus.Fields{
			"fullnode": fullnode,
			"space":    space,
			"method":   method,
			"args":     args,
		})

		logger.Debug("RPC enter")

		start := time.Now()
		err := handler(result, method, args...)
		logger = logger.WithField("elapsed", time.Since(start))

		if err != nil {
			logger = logger.WithError(err)
		}

		if logrus.IsLevelEnabled(logrus.TraceLevel) {
			logger = logger.WithField("result", result)
		}

		logger.Debug("RPC leave")

		return err
	}
}
