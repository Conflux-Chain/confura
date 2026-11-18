package ratelimit

import (
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
		RunE:  addStrategy,
	}

	removeStrategyCmd = &cobra.Command{
		Use:   "rms",
		Short: "Remove rate limit strategy",
		RunE:  delStrategy,
	}

	listStrategiesCmd = &cobra.Command{
		Use:   "lss",
		Short: "List rate limit strategies",
		RunE:  listStrategies,
	}
)

func init() {
	util.AddConfirmationFlag(addStrategyCmd)
	util.AddConfirmationFlag(removeStrategyCmd)

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

func addStrategy(cmd *cobra.Command, args []string) error {
	strategy, err := validateStrategyCmdConfig(true, true)
	if err != nil {
		return fmt.Errorf("add strategy: %w", err)
	}

	dbs, err := util.OpenManagementStore(stratCfg.Network)
	if err != nil {
		return fmt.Errorf("add strategy: %w", err)
	}

	defer dbs.Close()

	name := mysql.RateLimitStrategyConfKeyPrefix + stratCfg.Name
	snapshot, err := dbs.LoadConfigSnapshot(name)
	if err != nil {
		return fmt.Errorf("add strategy: %w", err)
	}

	op := "add a new rate limit strategy"
	if snapshot != nil {
		op = "update an existed rate limit strategy"
	}

	if err := util.ConfirmWrite(cmd, stratCfg.Network, op, strategy); err != nil {
		return err
	}

	if err := dbs.StoreConfigIfUnchanged(name, stratCfg.Rules, snapshot); err != nil {
		return fmt.Errorf("add strategy: %w", err)
	}

	logrus.WithField("strategy", strategy.Name).Info("Succeeded to ", op)
	return nil
}

func delStrategy(cmd *cobra.Command, args []string) error {
	_, err := validateStrategyCmdConfig(true, false)
	if err != nil {
		return fmt.Errorf("delete strategy: %w", err)
	}

	dbs, err := util.OpenManagementStore(stratCfg.Network)
	if err != nil {
		return fmt.Errorf("delete strategy: %w", err)
	}

	defer dbs.Close()

	name := mysql.RateLimitStrategyConfKeyPrefix + stratCfg.Name
	snapshot, err := dbs.LoadConfigSnapshot(name)
	if err != nil {
		return err
	}
	if snapshot == nil {
		cmd.Println("Configuration does not exist")
		return nil
	}

	if err := util.ConfirmWrite(cmd, stratCfg.Network, "delete configuration", snapshot); err != nil {
		return err
	}

	removed, err := dbs.DeleteConfigChecked(name, snapshot)
	if err != nil {
		return fmt.Errorf("delete strategy: %w", err)
	}

	if removed {
		logrus.WithField("strategy", stratCfg.Name).Info("Rate limit strategy deleted")
	} else {
		logrus.WithField("strategy", stratCfg.Name).Info("Rate limit strategy not existed")
	}
	return nil
}

func listStrategies(cmd *cobra.Command, args []string) error {
	dbs, err := util.OpenManagementStore(stratCfg.Network)
	if err != nil {
		return fmt.Errorf("list strategies: %w", err)
	}

	defer dbs.Close()

	strategies, _, err := dbs.LoadRateLimitStrategyConfigs()
	if err != nil {
		return fmt.Errorf("list strategies: %w", err)
	}

	if len(strategies) == 0 {
		logrus.Info("No rate limit strategies found")
		return nil
	}

	logrus.WithField("total", len(strategies)).Info("Rate limit strategies loaded:")

	for i, s := range strategies {
		logrus.WithFields(logrus.Fields{
			"name":  s.Name,
			"ID":    s.ID,
			"rules": s.LimitOptions,
		}).Info("Strategy #", i)
	}
	return nil
}

func validateStrategyCmdConfig(validateName, validateRules bool) (*rate.Strategy, error) {
	if validateName && len(stratCfg.Name) == 0 {
		return nil, errors.New("name must not be empty")
	}

	if !validateRules {
		return nil, nil
	}

	stg := rate.NewStrategy(0, stratCfg.Name)
	if err := stg.DecodeRules([]byte(stratCfg.Rules), true); err != nil {
		return nil, errors.WithMessage(err, "invalid strategy rules config json")
	}

	return stg, nil
}
