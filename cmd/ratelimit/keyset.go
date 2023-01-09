package ratelimit

import (
	"fmt"
	"strings"

	"github.com/Conflux-Chain/confura/cmd/util"
	rate "github.com/Conflux-Chain/confura/util/rate/v2"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type keysetCmdConfig struct {
	Network   string         // RPC network space ("cfx" or "eth")
	Strategy  string         // rate limit strategy
	LimitKey  string         // rate limit key
	LimitType rate.LimitType // rate limit type (0 - by key, 1 - by IP)
}

var (
	keysetCfg keysetCmdConfig

	limitTypeMap = map[rate.LimitType]string{
		rate.LimitTypeByIp:  "byIp",
		rate.LimitTypeByKey: "byKey",
	}

	// update is not supported
	addKeyCmd = &cobra.Command{
		Use:   "addk",
		Short: "Add rate limit key",
		Run:   addKey,
	}

	delKeyCmd = &cobra.Command{
		Use:   "rmk",
		Short: "Remove rate limit key",
		Run:   delKey,
	}

	listKeysCmd = &cobra.Command{
		Use:   "lsk",
		Short: "List rate limit keys",
		Run:   listKeys,
	}

	genKeyCmd = &cobra.Command{
		Use:   "gk",
		Short: "Generate random rate limit key",
		Run:   genKey,
	}
)

func init() {
	Cmd.AddCommand(addKeyCmd)
	hookKeysetCmdFlags(addKeyCmd, true, true, false, true)
	hookKeysetCmdLimitKeyFlag(addKeyCmd, false)

	Cmd.AddCommand(delKeyCmd)
	hookKeysetCmdFlags(delKeyCmd, true, false, true, false)

	Cmd.AddCommand(listKeysCmd)
	hookKeysetCmdFlags(listKeysCmd, true, true, false, false)

	Cmd.AddCommand(genKeyCmd)
	hookKeysetCmdFlags(genKeyCmd, false, false, false, true)
}

func addKey(cmd *cobra.Command, args []string) {
	storeCtx := util.MustInitStoreContext()
	defer storeCtx.Close()

	err := validateKeysetCmdConfig(true, true, false, true)
	if err != nil {
		logrus.WithField("config", keysetCfg).WithError(err).Info("Invalid command config")
		return
	}

	dbs := getMysqlStore(&storeCtx, keysetCfg.Network)
	if dbs == nil {
		logrus.Info("DB store is unavailable")
		return
	}

	strategy, err := dbs.LoadRateLimitStrategy(keysetCfg.Strategy)
	if err != nil {
		logrus.Info("Invalid rate limit strategy")
		return
	}

	limitKey := strings.TrimSpace(keysetCfg.LimitKey)
	if len(limitKey) == 0 { // generate random limit key if not provided
		limitKey, err = rate.GenerateRandomLimitKey(keysetCfg.LimitType)
		if err != nil {
			logrus.WithError(err).Info("Failed to generate random limit key")
			return
		}
	}

	logrus.WithFields(logrus.Fields{
		"strategyID":    strategy.ID,
		"strategyName":  strategy.Name,
		"strategyRules": strategy.LimitOptions,
		"limitKey":      limitKey,
		"limitType":     limitTypeMap[keysetCfg.LimitType],
	}).Info("Press the Enter Key to add new rate limit key")
	fmt.Scanln() // wait for Enter Key

	err = dbs.RateLimitStore.AddRateLimit(strategy.ID, keysetCfg.LimitType, limitKey)
	if err != nil {
		logrus.WithField("limitType", keysetCfg.LimitType).Info("Failed to add rate limit key")
		return
	}

	logrus.Info("New rate limit key added")
}

func delKey(cmd *cobra.Command, args []string) {
	storeCtx := util.MustInitStoreContext()
	defer storeCtx.Close()

	err := validateKeysetCmdConfig(true, false, true, false)
	if err != nil {
		logrus.WithField("config", keysetCfg).WithError(err).Info("Invalid command config")
		return
	}

	dbs := getMysqlStore(&storeCtx, keysetCfg.Network)
	if dbs == nil {
		logrus.Info("DB store is unavailable")
		return
	}

	logrus.WithField("limitKey", keysetCfg.LimitKey).
		Info("Press the Enter Key to delete the rate limit key!")
	fmt.Scanln() // wait for Enter Key

	removed, err := dbs.DeleteRateLimit(keysetCfg.LimitKey)
	if err != nil {
		logrus.WithError(err).Info("Failed to delete the rate limit key")
		return
	}

	if removed {
		logrus.WithField("limitKey", keysetCfg.LimitKey).Info("Rate limit key deleted")
	} else {
		logrus.WithField("limitKey", keysetCfg.LimitKey).Info("Rate limit key not existed")
	}
}

func listKeys(cmd *cobra.Command, args []string) {
	storeCtx := util.MustInitStoreContext()
	defer storeCtx.Close()

	err := validateKeysetCmdConfig(true, true, false, false)
	if err != nil {
		logrus.WithField("config", keysetCfg).WithError(err).Info("Invalid command config")
		return
	}

	dbs := getMysqlStore(&storeCtx, keysetCfg.Network)
	if dbs == nil {
		logrus.Info("DB store is unavailable")
		return
	}

	strategy, err := dbs.LoadRateLimitStrategy(keysetCfg.Strategy)
	if err != nil {
		logrus.Info("Invalid rate limit strategy")
		return
	}

	keyinfos, err := dbs.LoadRateLimitKeyset(&rate.KeysetFilter{
		SIDs:  []uint32{strategy.ID},
		Limit: 50,
	})
	if err != nil {
		logrus.WithError(err).Info("Failed to load rate limit keys")
		return
	}

	if len(keyinfos) == 0 {
		logrus.Info("No rate limit keys found")
		return
	}

	logrus.WithField("total", len(keyinfos)).Info("Rate limit keys loaded:")

	for i, k := range keyinfos {
		logrus.WithFields(logrus.Fields{
			"strategy":  strategy.Name,
			"limitKey":  k.Key,
			"limitType": limitTypeMap[k.Type],
		}).Info("Key #", i)
	}
}

func genKey(cmd *cobra.Command, args []string) {
	err := validateKeysetCmdConfig(false, false, false, true)
	if err != nil {
		logrus.WithField("config", keysetCfg).WithError(err).Info("Invalid command config")
		return
	}

	limitKey, err := rate.GenerateRandomLimitKey(keysetCfg.LimitType)
	if err != nil {
		logrus.Info("Failed to generate random limit key")
		return
	}

	logrus.WithFields(logrus.Fields{
		"limitKey":  limitKey,
		"limitType": limitTypeMap[keysetCfg.LimitType],
	}).Info("New random rate limit key generated")
}

func validateKeysetCmdConfig(validateNet, validateStrategy, validateLimitKey, validateLimitType bool) error {
	if validateNet {
		if err := validateNetwork(keysetCfg.Network); err != nil {
			return err
		}
	}

	if validateStrategy && len(keysetCfg.Strategy) == 0 {
		return errors.New("rate limit strategy must not be empty")
	}

	if validateLimitKey && len(keysetCfg.LimitKey) == 0 {
		return errors.New("rate limit key must not be empty")
	}

	if !validateLimitType {
		return nil
	}

	if keysetCfg.LimitType != rate.LimitTypeByIp && keysetCfg.LimitType != rate.LimitTypeByKey {
		return errors.New("invalid rate limit type")
	}

	return nil
}

func hookKeysetCmdFlags(keysetCmd *cobra.Command, hookNetwork, hookStrategy, hookLimitKey, hookLimitType bool) {
	if hookNetwork { // RPC network space
		keysetCmd.Flags().StringVarP(
			&keysetCfg.Network, "network", "n", "cfx", "RPC network space ('cfx' or 'eth')",
		)
		keysetCmd.MarkFlagRequired("network")
	}

	if hookStrategy { // strategy
		keysetCmd.Flags().StringVarP(
			&keysetCfg.Strategy, "strategy", "s", "", "strategy used",
		)
		keysetCmd.MarkFlagRequired("strategy")
	}

	if hookLimitKey { // rate limit key
		hookKeysetCmdLimitKeyFlag(keysetCmd, true)
	}

	if hookLimitType { // rate limit type
		keysetCmd.Flags().IntVarP(
			(*int)(&keysetCfg.LimitType),
			"type", "t", 0,
			"rate limit type (0 - by Key, 1 - by IP)",
		)
		keysetCmd.MarkFlagRequired("type")
	}
}

func hookKeysetCmdLimitKeyFlag(keysetCmd *cobra.Command, required bool) {
	keysetCmd.Flags().StringVarP(
		&keysetCfg.LimitKey, "key", "k", "", "rate limit key",
	)

	if required {
		keysetCmd.MarkFlagRequired("key")
	}
}
