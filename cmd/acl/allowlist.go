package acl

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Conflux-Chain/confura/cmd/util"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/util/acl"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/ethereum/go-ethereum/common"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type allowListCmdConfig struct {
	Name    string // allowlist name
	Network string // RPC network space ("cfx" or "eth")
	Rules   string // allowlist rules config json
}

var (
	alCfg allowListCmdConfig

	addAllowListCmd = &cobra.Command{
		Use:   "addal",
		Short: "Add or update access control allow list",
		Run:   addAllowList,
	}

	removeAllowListCmd = &cobra.Command{
		Use:   "rmal",
		Short: "Remove access control allow list",
		Run:   delAllowList,
	}

	listAllowListCmd = &cobra.Command{
		Use:   "lsal",
		Short: "List access controls allow lists",
		Run:   listAllowLists,
	}
)

func init() {
	Cmd.AddCommand(addAllowListCmd)
	hookAllowListCmdFlags(addAllowListCmd, true, true)

	Cmd.AddCommand(removeAllowListCmd)
	hookAllowListCmdFlags(removeAllowListCmd, true, false)

	Cmd.AddCommand(listAllowListCmd)
	hookAllowListCmdFlags(listAllowListCmd, false, false)
}

func hookAllowListCmdFlags(cmd *cobra.Command, hookName, hookRules bool) {
	{ // RPC network space
		cmd.Flags().StringVarP(
			&alCfg.Network, "network", "n", "cfx", "RPC network space ('cfx' or 'eth')",
		)
		cmd.MarkFlagRequired("network")
	}

	if hookName { // allowlist name
		cmd.Flags().StringVarP(
			&alCfg.Name, "name", "a", "", "allowlist name",
		)
		cmd.MarkFlagRequired("name")
	}

	if hookRules { // allowlist rules json
		cmd.Flags().StringVarP(
			&alCfg.Rules, "rules", "r", "", "allowlist rules config json",
		)
		cmd.MarkFlagRequired("rules")
	}
}

func addAllowList(cmd *cobra.Command, args []string) {
	storeCtx := util.MustInitStoreContext()
	defer storeCtx.Close()

	allowList, err := validateAllowListCmdConfig(true, true)
	if err != nil {
		logrus.WithField("config", alCfg).WithError(err).Info("Invalid command config")
		return
	}

	dbs, err := storeCtx.GetMysqlStore(alCfg.Network)
	if err != nil {
		logrus.WithError(err).Info("Failed to get mysql store by network")
		return
	}

	if dbs == nil {
		logrus.Info("DB store is unavailable")
		return
	}

	name := mysql.AclAllowListConfKeyPrefix + allowList.Name
	cfgmap, err := dbs.LoadConfig(name)
	if err != nil {
		logrus.WithField("allowList", allowList.Name).
			WithError(err).
			Info("Failed to load allowlist config")
		return
	}

	op := "add a new allowlist"
	if len(cfgmap) > 0 {
		op = "update an existed allowlist"
	}

	logrus.WithField("allowlist", *allowList).Info("Press the Enter Key to ", op)
	fmt.Scanln() // wait for Enter Key

	if err := dbs.StoreConfig(name, alCfg.Rules); err != nil {
		logrus.WithError(err).Info("Failed to ", op)
		return
	}

	logrus.WithField("allowlist", allowList.Name).Info("Succeeded to ", op)
}

func delAllowList(cmd *cobra.Command, args []string) {
	storeCtx := util.MustInitStoreContext()
	defer storeCtx.Close()

	_, err := validateAllowListCmdConfig(true, false)
	if err != nil {
		logrus.WithField("config", alCfg).WithError(err).Info("Invalid command config")
		return
	}

	dbs, err := storeCtx.GetMysqlStore(alCfg.Network)
	if err != nil {
		logrus.WithError(err).Info("Failed to get mysql store by network")
		return
	}

	if dbs == nil {
		logrus.Info("DB store is unavailable")
		return
	}

	logrus.WithField("allowList", alCfg.Name).
		Info("Press the Enter Key to delete the allowlist!")
	fmt.Scanln() // wait for Enter Key

	name := mysql.AclAllowListConfKeyPrefix + alCfg.Name
	removed, err := dbs.DeleteConfig(name)
	if err != nil {
		logrus.WithError(err).Info("Failed to delete the allow list")
		return
	}

	if removed {
		logrus.WithField("allowList", alCfg.Name).Info("Access control allowlist deleted")
	} else {
		logrus.WithField("allowList", alCfg.Name).Info("Access control allowlist not existed")
	}
}

func listAllowLists(cmd *cobra.Command, args []string) {
	storeCtx := util.MustInitStoreContext()
	defer storeCtx.Close()

	dbs, err := storeCtx.GetMysqlStore(alCfg.Network)
	if err != nil {
		logrus.WithError(err).Info("Failed to get mysql store by network")
		return
	}

	if dbs == nil {
		logrus.Info("DB store is unavailable")
		return
	}

	allowLists, _, err := dbs.LoadAclAllowListConfigs()
	if err != nil {
		logrus.WithError(err).Info("Failed to load access control allowlist config")
		return
	}

	if len(allowLists) == 0 {
		logrus.Info("No allowlist found")
		return
	}

	logrus.WithField("total", len(allowLists)).Info("Access control allowlists loaded:")

	for i, al := range allowLists {
		logrus.WithField("allowList", *al).Info("Allowlist #", i)
	}
}

func validateAllowListCmdConfig(validateName, validateRules bool) (*acl.AllowList, error) {
	if validateName && len(alCfg.Name) == 0 {
		return nil, errors.New("name must not be empty")
	}

	if !validateRules {
		return nil, nil
	}

	al := acl.NewAllowList(0, alCfg.Name)
	if err := json.Unmarshal([]byte(alCfg.Rules), al); err != nil {
		return nil, errors.WithMessage(err, "invalid allowlist rules config json")
	}

	if len(al.AllowMethods) > 0 && len(al.DisallowMethods) > 0 {
		return nil, errors.New("The allow and disallow method sets can not be set at the same time")
	}

	if err := validateAllowListContractAddresses(al); err != nil {
		return nil, errors.WithMessage(err, "invalid allowlist contract addresses")
	}

	return al, nil
}

func validateAllowListContractAddresses(al *acl.AllowList) error {
	if strings.EqualFold(alCfg.Network, "eth") {
		for _, caddr := range al.ContractAddresses {
			if !common.IsHexAddress(caddr) {
				return errors.Errorf("%v is not a hex address", caddr)
			}
		}
		return nil
	}

	if strings.EqualFold(alCfg.Network, "cfx") {
		for _, ctAddr := range al.ContractAddresses {
			if _, err := cfxaddress.NewFromBase32(ctAddr); err != nil {
				return errors.WithMessagef(err, "%v is not a valid base32 string", ctAddr)
			}
		}
	}

	return nil
}
