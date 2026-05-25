package cmd

import (
	"fmt"
	"strings"

	cmdutil "github.com/Conflux-Chain/confura/cmd/util"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type pruneCmdConfig struct {
	Network string
	Yes     bool
}

var (
	pruneCfg pruneCmdConfig

	pruneCmd = &cobra.Command{
		Use:   "prune",
		Short: "Prune archive log partitions manually",
		Run:   pruneArchiveLogPartitions,
	}
)

func init() {
	pruneCmd.Flags().StringVarP(
		&pruneCfg.Network, "network", "n", "all", "network space to prune ('cfx', 'eth' or 'all')",
	)
	pruneCmd.Flags().BoolVarP(
		&pruneCfg.Yes, "yes", "y", false, "skip confirmation prompt",
	)

	rootCmd.AddCommand(pruneCmd)
}

func pruneArchiveLogPartitions(cmd *cobra.Command, args []string) {
	network := strings.ToLower(pruneCfg.Network)
	if network != "cfx" && network != "eth" && network != "all" {
		logrus.WithField("network", pruneCfg.Network).Error("Invalid network, expected 'cfx', 'eth' or 'all'")
		return
	}

	if !pruneCfg.Yes {
		logrus.WithField("network", network).
			Info("Press the Enter Key to prune archive log partitions")
		fmt.Scanln()
	}

	storeCtx := cmdutil.MustInitStoreContext()
	defer storeCtx.Close()

	var totalPruned int
	var totalEntities int

	if network == "cfx" || network == "all" {
		if storeCtx.CfxDB == nil {
			logrus.Info("Core space DB store is unavailable")
		} else {
			entities, pruned := logArchiveLogPruneResults("cfx", storeCtx.CfxDB.PruneArchiveLogPartitions())
			totalEntities += entities
			totalPruned += pruned
		}
	}

	if network == "eth" || network == "all" {
		if storeCtx.EthDB == nil {
			logrus.Info("EVM space DB store is unavailable")
		} else {
			entities, pruned := logArchiveLogPruneResults("eth", storeCtx.EthDB.PruneArchiveLogPartitions())
			totalEntities += entities
			totalPruned += pruned
		}
	}

	logrus.WithFields(logrus.Fields{
		"entities":          totalEntities,
		"prunedPartitions":  totalPruned,
		"configuredNetwork": network,
	}).Info("Archive log partition prune completed")
}

func logArchiveLogPruneResults(network string, results []mysql.ArchiveLogPruneResult) (int, int) {
	if len(results) == 0 {
		logrus.WithField("network", network).Info("No archive log partitions exceeded the configured limit")
		return 0, 0
	}

	var entities, pruned int
	for _, result := range results {
		logger := logrus.WithFields(logrus.Fields{
			"network": network,
			"entity":  result.Entity,
		})

		if result.Err != nil {
			logger.WithError(result.Err).Error("Failed to prune archive log partitions")
			continue
		}

		entities++
		pruned += len(result.PrunedPartitionIndexes)
		logger.WithField("prunedPartitionIndexes", result.PrunedPartitionIndexes).
			Info("Archive log partitions pruned")
	}

	return entities, pruned
}
