package ratelimit

import (
	"encoding/json"
	"fmt"

	"github.com/Conflux-Chain/confura/cmd/util"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/util/rate"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type strategyCmdConfig struct {
	Name    string // strategy name
	Network string // RPC network space ("cfx" or "eth")
	Rules   string // strategy rules config json
}

var (
	stratCfg strategyCmdConfig

	addStrategyCmd = &cobra.Command{
		Use:   "adds",
		Short: "Add or update rate limit strategy",
		Run:   addStrategy,
	}

	removeStrategyCmd = &cobra.Command{
		Use:   "rms",
		Short: "Remove rate limit strategy",
		Run:   delStrategy,
	}

	listStrategiesCmd = &cobra.Command{
		Use:   "lss",
		Short: "List rate limit strategies",
		Run:   listStrategies,
	}
)

func init() {
	Cmd.AddCommand(addStrategyCmd)
	hookStrategyCmdFlags(addStrategyCmd, true, true)

	Cmd.AddCommand(removeStrategyCmd)
	hookStrategyCmdFlags(removeStrategyCmd, true, false)

	Cmd.AddCommand(listStrategiesCmd)
	hookStrategyCmdFlags(listStrategiesCmd, false, false)
}

func hookStrategyCmdFlags(stratCmd *cobra.Command, hookName, hookRules bool) {
	{ // RPC network space
		stratCmd.Flags().StringVarP(
			&stratCfg.Network, "network", "n", "cfx", "RPC network space ('cfx' or 'eth')",
		)
		stratCmd.MarkFlagRequired("network")
	}

	if hookName { // strategy name
		stratCmd.Flags().StringVarP(
			&stratCfg.Name, "name", "a", "", "strategy name",
		)
		stratCmd.MarkFlagRequired("name")
	}

	if hookRules { // strategy rules json
		stratCmd.Flags().StringVarP(
			&stratCfg.Rules, "rules", "r", "", "strategy rules config json",
		)
		stratCmd.MarkFlagRequired("rules")
	}
}

func addStrategy(cmd *cobra.Command, args []string) {
	storeCtx := util.MustInitStoreContext()
	defer storeCtx.Close()

	strategy, err := validateStrategyCmdConfig(true, true)
	if err != nil {
		logrus.WithField("config", stratCfg).WithError(err).Info("Invalid command config")
		return
	}

	dbs, err := storeCtx.GetMysqlStore(stratCfg.Network)
	if err != nil {
		logrus.WithError(err).Info("Failed to get mysql store by network")
		return
	}

	if dbs == nil {
		logrus.Info("DB store is unavailable")
		return
	}

	name := mysql.RateLimitStrategyConfKeyPrefix + stratCfg.Name
	cfgmap, err := dbs.LoadConfig(name)
	if err != nil {
		logrus.WithField("strategy", stratCfg.Name).
			WithError(err).
			Info("Failed to load rate limit config")
		return
	}

	op := "add a new rate limit strategy"
	if len(cfgmap) > 0 {
		op = "update an existed rate limit strategy"
	}

	logrus.WithFields(logrus.Fields{
		"name":  strategy.Name,
		"rules": strategy.LimitOptions,
	}).Info("Press the Enter Key to ", op)
	fmt.Scanln() // wait for Enter Key

	if err := dbs.StoreConfig(name, stratCfg.Rules); err != nil {
		logrus.WithError(err).Info("Failed to ", op)
		return
	}

	logrus.WithField("strategy", strategy.Name).Info("Succeeded to ", op)
}

func delStrategy(cmd *cobra.Command, args []string) {
	storeCtx := util.MustInitStoreContext()
	defer storeCtx.Close()

	_, err := validateStrategyCmdConfig(true, false)
	if err != nil {
		logrus.WithField("config", stratCfg).WithError(err).Info("Invalid command config")
		return
	}

	dbs, err := storeCtx.GetMysqlStore(stratCfg.Network)
	if err != nil {
		logrus.WithError(err).Info("Failed to get mysql store by network")
		return
	}

	if dbs == nil {
		logrus.Info("DB store is unavailable")
		return
	}

	logrus.WithField("strategy", stratCfg.Name).
		Info("Press the Enter Key to delete the rate limit strategy!")
	fmt.Scanln() // wait for Enter Key

	name := mysql.RateLimitStrategyConfKeyPrefix + stratCfg.Name
	removed, err := dbs.DeleteConfig(name)
	if err != nil {
		logrus.WithError(err).Info("Failed to delete the rate limit strategy")
		return
	}

	if removed {
		logrus.WithField("strategy", stratCfg.Name).Info("Rate limit strategy deleted")
	} else {
		logrus.WithField("strategy", stratCfg.Name).Info("Rate limit strategy not existed")
	}
}

func listStrategies(cmd *cobra.Command, args []string) {
	storeCtx := util.MustInitStoreContext()
	defer storeCtx.Close()

	dbs, err := storeCtx.GetMysqlStore(stratCfg.Network)
	if err != nil {
		logrus.WithError(err).Info("Failed to get mysql store by network")
		return
	}

	if dbs == nil {
		logrus.Info("DB store is unavailable")
		return
	}

	config, err := dbs.LoadRateLimitConfigs()
	if err != nil {
		logrus.WithError(err).Info("Failed to load rate limit configs")
		return
	}

	if len(config.Strategies) == 0 {
		logrus.Info("No rate limit strategies found")
		return
	}

	logrus.WithField("total", len(config.Strategies)).Info("Rate limit strategies loaded:")

	for i, s := range config.Strategies {
		logrus.WithFields(logrus.Fields{
			"name":  s.Name,
			"ID":    s.ID,
			"rules": s.LimitOptions,
		}).Info("Strategy #", i)
	}
}

func validateStrategyCmdConfig(validateName, validateRules bool) (*rate.Strategy, error) {
	if validateName && len(stratCfg.Name) == 0 {
		return nil, errors.New("name must not be empty")
	}

	if !validateRules {
		return nil, nil
	}

	stg := rate.NewStrategy(0, stratCfg.Name)
	if err := json.Unmarshal([]byte(stratCfg.Rules), stg); err != nil {
		return nil, errors.WithMessage(err, "invalid strategy rules config json")
	}

	return stg, nil
}
