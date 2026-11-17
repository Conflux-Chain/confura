package noderoute

import (
	"errors"
	"fmt"

	"github.com/Conflux-Chain/confura/cmd/util"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type routeCmdConfig struct {
	Network string // network space ("cfx" or "eth")
	Key     string // route key
	Group   string // route group
}

var (
	routeCfg routeCmdConfig

	// update is not supported

	addRouteCmd = &cobra.Command{
		Use:   "add",
		Short: "Add new route",
		RunE:  addRoute,
	}

	delRouteCmd = &cobra.Command{
		Use:   "rm",
		Short: "Remove existing route",
		RunE:  delRoute,
	}

	listRoutesCmd = &cobra.Command{
		Use:   "ls",
		Short: "List all available routes",
		RunE:  listRoutes,
	}
)

func init() {
	Cmd.AddCommand(addRouteCmd)
	hookRouteCmdFlags(addRouteCmd, true, true)

	Cmd.AddCommand(delRouteCmd)
	hookRouteCmdFlags(delRouteCmd, true, false)

	Cmd.AddCommand(listRoutesCmd)
	hookRouteCmdFlags(listRoutesCmd, false, false)
}

func addRoute(cmd *cobra.Command, args []string) error {
	err := validateRouteCmdConfig(true, true)
	if err != nil {
		return fmt.Errorf("add route: %w", err)
	}

	dbs, err := util.OpenManagementStore(routeCfg.Network)
	if err != nil {
		return fmt.Errorf("add route: %w", err)
	}

	defer dbs.Close()

	routeGroups, err := dbs.LoadNodeRouteGroups(routeCfg.Group)
	if err != nil {
		return fmt.Errorf("add route: %w", err)
	}

	if _, ok := routeGroups[routeCfg.Group]; !ok {
		logrus.WithField("group", routeCfg.Group).Info(
			fmt.Sprintf("!!! %v, %v",
				"The provided route group doesn't exist in db yet",
				"you might add it with node manager RPC at first",
			))
	}

	logrus.WithFields(logrus.Fields{
		"routeKey":   routeCfg.Key,
		"routeGroup": routeGroups[routeCfg.Group],
	}).Info("Press the Enter Key to add new node route")

	fmt.Scanln() // wait for Enter Key

	if err := dbs.AddNodeRoute(routeCfg.Key, routeCfg.Group); err != nil {
		return fmt.Errorf("add route: %w", err)
	}

	logrus.Info("New node route added")
	return nil
}

func delRoute(cmd *cobra.Command, args []string) error {
	err := validateRouteCmdConfig(true, false)
	if err != nil {
		return fmt.Errorf("delete route: %w", err)
	}

	dbs, err := util.OpenManagementStore(routeCfg.Network)
	if err != nil {
		return fmt.Errorf("delete route: %w", err)
	}

	defer dbs.Close()

	logrus.WithField("routeKey", routeCfg.Key).Info("Press Enter Key to delete the node route!")
	fmt.Scanln() // wait for Enter Key

	removed, err := dbs.DeleteNodeRoute(routeCfg.Key)
	if err != nil {
		return fmt.Errorf("delete route: %w", err)
	}

	if removed {
		logrus.Info("Node route deleted")
	} else {
		logrus.Info("Node route does not exist")
	}
	return nil
}

func listRoutes(cmd *cobra.Command, args []string) error {
	dbs, err := util.OpenManagementStore(routeCfg.Network)
	if err != nil {
		return fmt.Errorf("list routes: %w", err)
	}

	defer dbs.Close()

	nodeRoutes, err := dbs.LoadNodeRoutes(mysql.NodeRouteFilter{})
	if err != nil {
		return fmt.Errorf("list routes: %w", err)
	}

	if len(nodeRoutes) == 0 {
		logrus.Info("No node route found")
		return nil
	}

	logrus.WithField("total", len(nodeRoutes)).Info("Node routes loaded:")

	for i, route := range nodeRoutes {
		logrus.WithFields(logrus.Fields{
			"id":         route.ID,
			"routeKey":   route.RouteKey,
			"routeGroup": route.Group,
		}).Info("Route #", i)
	}
	return nil
}

func validateRouteCmdConfig(validateKey, validateGroup bool) error {
	if validateKey && len(routeCfg.Key) == 0 {
		return errors.New("route key must not be empty")
	}

	if validateGroup && len(routeCfg.Group) == 0 {
		return errors.New("route group must not be empty")
	}

	return nil
}

func hookRouteCmdFlags(routeCmd *cobra.Command, hookRouteKey, hookRouteGroup bool) {
	{ // network space
		routeCmd.Flags().StringVarP(
			&routeCfg.Network, "network", "n", "cfx", "network space ('cfx' or 'eth')",
		)
		routeCmd.MarkFlagRequired("network")
	}

	if hookRouteKey { // route key
		routeCmd.Flags().StringVarP(
			&routeCfg.Key, "key", "k", "", "route key",
		)
		routeCmd.MarkFlagRequired("key")
	}

	if hookRouteGroup { // route group
		routeCmd.Flags().StringVarP(
			&routeCfg.Group, "group", "g", "", "route group",
		)
		routeCmd.MarkFlagRequired("group")
	}
}
