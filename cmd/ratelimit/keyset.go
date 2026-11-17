package ratelimit

import (
	"fmt"
	"strings"

	"github.com/Conflux-Chain/confura/cmd/util"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/util/acl"
	"github.com/Conflux-Chain/confura/util/rate"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type keysetCmdConfig struct {
	Network   string         // RPC network space ("cfx" or "eth")
	Strategy  string         // rate limit strategy
	AllowList string         // acl allow list
	LimitKey  string         // rate limit key
	LimitType rate.LimitType // rate limit type (0 - by key, 1 - by IP)
	Memo      string         // rate limit memo
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
		RunE:  addKey,
	}

	delKeyCmd = &cobra.Command{
		Use:   "rmk",
		Short: "Remove rate limit key",
		RunE:  delKey,
	}

	listKeysCmd = &cobra.Command{
		Use:   "lsk",
		Short: "List rate limit keys",
		RunE:  listKeys,
	}

	genKeyCmd = &cobra.Command{
		Use:   "gk",
		Short: "Generate random rate limit key",
		RunE:  genKey,
	}
)

func init() {
	util.AddConfirmationFlag(addKeyCmd)
	util.AddConfirmationFlag(delKeyCmd)

	Cmd.AddCommand(addKeyCmd)
	hookKeysetCmdFlags(addKeyCmd, true, true, false, true)
	hookKeysetCmdLimitKeyFlag(addKeyCmd, false)
	hookKeysetCmdMemoFlag(addKeyCmd)
	hookKeysetCmdAllowListFlag(addKeyCmd)

	Cmd.AddCommand(delKeyCmd)
	hookKeysetCmdFlags(delKeyCmd, true, false, true, false)

	Cmd.AddCommand(listKeysCmd)
	hookKeysetCmdFlags(listKeysCmd, true, true, false, false)

	Cmd.AddCommand(genKeyCmd)
	hookKeysetCmdFlags(genKeyCmd, false, false, false, true)
}

func addKey(cmd *cobra.Command, args []string) error {
	err := validateKeysetCmdConfig(true, false, true)
	if err != nil {
		return fmt.Errorf("add key: %w", err)
	}

	dbs, err := util.OpenManagementStore(keysetCfg.Network)
	if err != nil {
		return fmt.Errorf("add key: %w", err)
	}

	defer dbs.Close()

	_, strategySnapshot, err := dbs.LoadRateLimitStrategySnapshot(keysetCfg.Strategy)
	if err != nil {
		return fmt.Errorf("add key: %w", err)
	}

	var acl acl.AllowList
	var aclSnapshot *mysql.ConfigSnapshot
	if len(keysetCfg.AllowList) > 0 {
		allowList, snapshot, err := dbs.LoadAclAllowListSnapshot(keysetCfg.AllowList)
		if err != nil {
			return fmt.Errorf("add key: %w", err)
		}

		acl = *allowList
		acl.ID = snapshot.ID
		aclSnapshot = snapshot
	}

	limitKey := strings.TrimSpace(keysetCfg.LimitKey)
	if len(limitKey) == 0 { // generate random limit key if not provided
		limitKey, err = rate.GenerateRandomLimitKey(keysetCfg.LimitType)
		if err != nil {
			return fmt.Errorf("add key: %w", err)
		}
	}

	if err := util.ConfirmWrite(cmd, keysetCfg.Network, "add access key", struct {
		Key, Type, Memo string
		Strategy, ACL   *mysql.ConfigSnapshot
	}{limitKey, limitTypeMap[keysetCfg.LimitType], keysetCfg.Memo, strategySnapshot, aclSnapshot}); err != nil {
		return err
	}

	err = dbs.RateLimitStore.AddRateLimit(
		strategySnapshot.ID, acl.ID, keysetCfg.LimitType, limitKey, keysetCfg.Memo, strategySnapshot, aclSnapshot,
	)
	if err != nil {
		return fmt.Errorf("add key: %w", err)
	}

	logrus.Info("New rate limit key added")
	return nil
}

func delKey(cmd *cobra.Command, args []string) error {
	err := validateKeysetCmdConfig(false, true, false)
	if err != nil {
		return fmt.Errorf("delete key: %w", err)
	}

	dbs, err := util.OpenManagementStore(keysetCfg.Network)
	if err != nil {
		return fmt.Errorf("delete key: %w", err)
	}

	defer dbs.Close()

	keys, err := dbs.LoadRateLimitKeyset(&rate.KeysetFilter{KeySet: []string{keysetCfg.LimitKey}})
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		cmd.Println("Rate limit key does not exist")
		return nil
	}
	if err := util.ConfirmWrite(cmd, keysetCfg.Network, "delete access key", keys[0]); err != nil {
		return err
	}
	removed, err := dbs.DeleteRateLimitChecked(keys[0])
	if err != nil {
		return fmt.Errorf("delete key: %w", err)
	}

	if removed {
		logrus.WithField("limitKey", keysetCfg.LimitKey).Info("Rate limit key deleted")
	} else {
		logrus.WithField("limitKey", keysetCfg.LimitKey).Info("Rate limit key not existed")
	}
	return nil
}

func listKeys(cmd *cobra.Command, args []string) error {
	err := validateKeysetCmdConfig(true, false, false)
	if err != nil {
		return fmt.Errorf("list keys: %w", err)
	}

	dbs, err := util.OpenManagementStore(keysetCfg.Network)
	if err != nil {
		return fmt.Errorf("list keys: %w", err)
	}

	defer dbs.Close()

	strategy, err := dbs.LoadRateLimitStrategy(keysetCfg.Strategy)
	if err != nil {
		return fmt.Errorf("list keys: %w", err)
	}

	keysets, err := dbs.LoadRateLimitKeyset(&rate.KeysetFilter{
		SIDs:  []uint32{strategy.ID},
		Limit: 50,
	})
	if err != nil {
		return fmt.Errorf("list keys: %w", err)
	}

	if len(keysets) == 0 {
		logrus.Info("No rate limit keys found")
		return nil
	}

	logrus.WithField("total", len(keysets)).Info("Rate limit keys loaded:")

	allowLists := make(map[uint32]*acl.AllowList)
	for i, k := range keysets {
		if k.AclID > 0 && allowLists[k.AclID] == nil {
			acl, err := dbs.LoadAclAllowListById(k.AclID)
			if err != nil {
				return fmt.Errorf("list keys: %w", err)
			}
			allowLists[k.AclID] = acl
		}

		logrus.WithFields(logrus.Fields{
			"strategy":  strategy.Name,
			"limitKey":  k.LimitKey,
			"limitType": limitTypeMap[rate.LimitType(k.LimitType)],
			"allowList": allowLists[k.AclID],
			"memo":      k.Memo,
		}).Info("Key #", i)
	}
	return nil
}

func genKey(cmd *cobra.Command, args []string) error {
	err := validateKeysetCmdConfig(false, false, true)
	if err != nil {
		return fmt.Errorf("generate key: %w", err)
	}

	limitKey, err := rate.GenerateRandomLimitKey(keysetCfg.LimitType)
	if err != nil {
		return fmt.Errorf("generate key: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"limitKey":  limitKey,
		"limitType": limitTypeMap[keysetCfg.LimitType],
	}).Info("New random rate limit key generated")
	return nil
}

func validateKeysetCmdConfig(validateStrategy, validateLimitKey, validateLimitType bool) error {
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

func hookKeysetCmdMemoFlag(keysetCmd *cobra.Command) {
	keysetCmd.Flags().StringVarP(
		&keysetCfg.Memo, "memo", "m", "", "rate limit memo",
	)
}

func hookKeysetCmdAllowListFlag(keysetCmd *cobra.Command) {
	keysetCmd.Flags().StringVarP(
		&keysetCfg.AllowList, "acl", "l", "", "allowlist used",
	)
}
