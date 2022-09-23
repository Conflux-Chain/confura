package ratelimit

import (
	"fmt"
	"strings"

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

	strategy, err := validateStrategyCmdConfig(true, true, true)
	if err != nil {
		logrus.WithField("config", stratCfg).WithError(err).Info("Invalid command config")
		return
	}

	dbs := getMysqlStore(&storeCtx)
	name := rate.ConfigStrategyPrefix + stratCfg.Name

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
		"name": strategy.Name, "rules": strategy.Rules,
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

	_, err := validateStrategyCmdConfig(true, true, false)
	if err != nil {
		logrus.WithField("config", stratCfg).WithError(err).Info("Invalid command config")
		return
	}

	logrus.WithField("strategy", stratCfg.Name).
		Info("Press the Enter Key to delete the rate limit strategy!")
	fmt.Scanln() // wait for Enter Key

	name := rate.ConfigStrategyPrefix + stratCfg.Name
	removed, err := getMysqlStore(&storeCtx).DeleteConfig(name)
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

	_, err := validateStrategyCmdConfig(false, true, false)
	if err != nil {
		logrus.WithField("config", stratCfg).WithError(err).Info("Invalid command config")
		return
	}

	config, err := getMysqlStore(&storeCtx).LoadRateLimitConfigs()
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
			"name": s.Name, "ID": s.ID, "rules": s.Rules,
		}).Info("Strategy #", i)
	}
}

func getMysqlStore(storeCtx *util.StoreContext) *mysql.MysqlStore {
	dbs := storeCtx.CfxDB
	if strings.EqualFold(stratCfg.Network, "eth") {
		dbs = storeCtx.EthDB
	}

	if dbs == nil {
		logrus.WithField("network", stratCfg.Network).Fatal("No DB store avaliable")
	}

	return dbs
}

func validateStrategyCmdConfig(validateName, validateNetwork, validateRules bool) (*rate.Strategy, error) {
	if validateName && len(stratCfg.Name) == 0 {
		return nil, errors.New("name must not be empty")
	}

	if validateNetwork && len(stratCfg.Network) == 0 {
		return nil, errors.New("network space must not be empty")
	}

	if !validateRules {
		return nil, nil
	}

	ruleOpts, err := rate.JsonUnmarshalStrategyRules([]byte(stratCfg.Rules))
	if err != nil {
		return nil, errors.WithMessage(err, "invalid strategy rules config json")
	}

	return &rate.Strategy{
		Name:  stratCfg.Name,
		Rules: ruleOpts,
	}, nil
}
