package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"golang.org/x/net/context"

	"github.com/SoftwareDefinedBuildings/btrdb/internal/configprovider"
	client "github.com/coreos/etcd/clientv3"
	"github.com/urfave/cli"
)

var ClusterAdminCommands = []cli.Command{
	{
		Name:        "status",
		Usage:       "show the cluster status",
		Description: "no really, there is a lot of dooing to be done",
		Category:    "v4 cluster admin",
		Action:      cli.ActionFunc(actionStatus),
	},
	{
		Name:     "disable",
		Action:   cli.ActionFunc(actionDisable),
		Category: "v4 cluster admin",
		Flags: []cli.Flag{
			cli.BoolFlag{Name: "force"},
		},
	},
	{
		Name:     "out",
		Category: "v4 cluster admin",
		Action:   cli.ActionFunc(actionOut),
	},
	{
		Name:     "enable",
		Category: "v4 cluster admin",
		Action:   cli.ActionFunc(actionEnable),
	},
	{
		Name:     "in",
		Category: "v4 cluster admin",
		Action:   cli.ActionFunc(actionIn),
	},
	{
		Name:     "rm",
		Category: "v4 cluster admin",
		Action:   cli.ActionFunc(actionRm),
	},
	{
		Name:     "weight",
		Category: "v4 cluster admin",
		Action:   cli.ActionFunc(actionWeight),
	},
	{
		Name:     "rpref",
		Category: "v4 cluster admin",
		Action:   cli.ActionFunc(actionRpref),
	},
}

func getclient(c *cli.Context) *client.Client {
	client, err := client.New(client.Config{
		Endpoints:   c.GlobalStringSlice("etcd"),
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		fmt.Printf("Could not establish connection to etcd: %v\n", err)
		os.Exit(2)
	}
	return client
}
func actionStatus(c *cli.Context) error {
	cc := getclient(c)
	cs, err := configprovider.QueryClusterState(context.Background(), cc, c.GlobalString("cluster"))
	if err != nil {
		fmt.Printf("Could not obtain cluster state: %v\n", err)
	} else {
		fmt.Println(cs.String())
	}
	return nil
}
func actionDisable(c *cli.Context) error {
	if len(c.Args()) != 1 {
		return cli.NewExitError("expected one node", 1)
	}
	nn := c.Args()[0]
	if nn == "" {
		return cli.NewExitError("No node specified", 1)
	}
	cc := getclient(c)
	cs, err := configprovider.QueryClusterState(context.Background(), cc, c.GlobalString("cluster"))
	if err != nil {
		fmt.Printf("could not obtain cluster state: %v\n", err)
		os.Exit(2)
	}
	m, ok := cs.Members[nn]
	if !ok {
		fmt.Printf("node '%s' does not exist\n", nn)
		os.Exit(1)
	}
	if m.In && !c.Bool("force") {
		fmt.Printf("cannot delete, node is IN\nplease OUT the node and wait for cluster to be Healthy\nor specify --force\n")
		os.Exit(1)
	}
	_, err = cc.Put(context.Background(), fmt.Sprintf("%s/x/m/%s/enabled", c.GlobalString("cluster"), nn), "false")
	if err != nil {
		fmt.Printf("Could not disable node: %v\n", err)
		os.Exit(2)
	}
	return nil
}
func actionOut(c *cli.Context) error {
	if len(c.Args()) != 1 {
		return cli.NewExitError("expected one node", 1)
	}
	nn := c.Args()[0]
	if nn == "" {
		return cli.NewExitError("No node specified", 1)
	}
	cc := getclient(c)
	cs, err := configprovider.QueryClusterState(context.Background(), cc, c.GlobalString("cluster"))
	if err != nil {
		fmt.Printf("Could not obtain cluster state: %v\n", err)
		os.Exit(2)
	}
	_, ok := cs.Members[nn]
	if !ok {
		fmt.Printf("node '%s' does not exist\n", nn)
		os.Exit(1)
	}
	_, err = cc.Put(context.Background(), fmt.Sprintf("%s/x/m/%s/in", c.GlobalString("cluster"), nn), "false")
	if err != nil {
		fmt.Printf("Could not out node: %v\n", err)
		os.Exit(2)
	}
	return nil
}
func actionEnable(c *cli.Context) error {
	if len(c.Args()) != 1 {
		return cli.NewExitError("expected one node", 1)
	}
	nn := c.Args()[0]
	if nn == "" {
		return cli.NewExitError("No node specified", 1)
	}
	cc := getclient(c)
	cs, err := configprovider.QueryClusterState(context.Background(), cc, c.GlobalString("cluster"))
	if err != nil {
		fmt.Printf("Could not obtain cluster state: %v\n", err)
		os.Exit(2)
	}
	_, ok := cs.Members[nn]
	if !ok {
		fmt.Printf("node '%s' does not exist\n", nn)
		os.Exit(1)
	}
	_, err = cc.Put(context.Background(), fmt.Sprintf("%s/x/m/%s/enabled", c.GlobalString("cluster"), nn), "true")
	if err != nil {
		fmt.Printf("Could not enable node: %v\n", err)
		os.Exit(2)
	}
	return nil

}
func actionIn(c *cli.Context) error {
	if len(c.Args()) != 1 {
		return cli.NewExitError("expected one node", 1)
	}
	nn := c.Args()[0]
	if nn == "" {
		return cli.NewExitError("No node specified", 1)
	}
	cc := getclient(c)
	cs, err := configprovider.QueryClusterState(context.Background(), cc, c.GlobalString("cluster"))
	if err != nil {
		fmt.Printf("Could not obtain cluster state: %v\n", err)
		os.Exit(2)
	}
	_, ok := cs.Members[nn]
	if !ok {
		fmt.Printf("node '%s' does not exist\n", nn)
		os.Exit(1)
	}
	_, err = cc.Put(context.Background(), fmt.Sprintf("%s/x/m/%s/in", c.GlobalString("cluster"), nn), "true")
	if err != nil {
		fmt.Printf("Could not in node: %v\n", err)
		os.Exit(2)
	}
	return nil
}
func actionRm(c *cli.Context) error {
	if len(c.Args()) != 1 {
		return cli.NewExitError("expected one node", 1)
	}
	nn := c.Args()[0]
	if nn == "" {
		return cli.NewExitError("No node specified", 1)
	}
	cc := getclient(c)
	cs, err := configprovider.QueryClusterState(context.Background(), cc, c.GlobalString("cluster"))
	if err != nil {
		fmt.Printf("Could not obtain cluster state: %v\n", err)
		os.Exit(2)
	}
	m, ok := cs.Members[nn]
	if !ok {
		fmt.Printf("node '%s' does not exist\n", nn)
		os.Exit(1)
	}
	if m.In {
		fmt.Printf("Cannot delete, node is IN\n")
		os.Exit(1)
	}
	if m.Enabled {
		fmt.Printf("Cannot delete, node is ENABLED\n")
		os.Exit(1)
	}
	_, err = cc.Delete(context.Background(), fmt.Sprintf("%s/x/m/%s", c.GlobalString("cluster"), nn), client.WithPrefix())
	if err != nil {
		fmt.Printf("Could not delete node: %v\n", err)
		os.Exit(2)
	}
	return nil
}
func actionWeight(c *cli.Context) error {
	if len(c.Args()) != 2 {
		return cli.NewExitError("expected nodename, weight", 1)
	}
	nn := c.Args()[0]
	if nn == "" {
		return cli.NewExitError("No node specified", 1)
	}
	weight, err := strconv.ParseUint(c.Args()[1], 10, 32)
	if err != nil {
		return cli.NewExitError("Bad weight, must be positive integer", 1)
	}
	cc := getclient(c)
	cs, err := configprovider.QueryClusterState(context.Background(), cc, c.GlobalString("cluster"))
	if err != nil {
		fmt.Printf("Could not obtain cluster state: %v\n", err)
		os.Exit(2)
	}
	_, ok := cs.Members[nn]
	if !ok {
		fmt.Printf("node '%s' does not exist\n", nn)
		os.Exit(1)
	}
	_, err = cc.Put(context.Background(), fmt.Sprintf("%s/x/m/%s/weight", c.GlobalString("cluster"), nn), strconv.FormatUint(weight, 10))
	if err != nil {
		fmt.Printf("Could not set node weight: %v\n", err)
		os.Exit(2)
	}
	return nil
}
func actionRpref(c *cli.Context) error {
	if len(c.Args()) != 2 {
		return cli.NewExitError("expected nodename, rpref", 1)
	}
	nn := c.Args()[0]
	if nn == "" {
		return cli.NewExitError("No node specified", 1)
	}
	rpref, err := strconv.ParseFloat(c.Args()[1], 64)
	if err != nil || rpref < 0 {
		return cli.NewExitError("Bad rpref, must be a float >= 0", 1)
	}
	cc := getclient(c)
	cs, err := configprovider.QueryClusterState(context.Background(), cc, c.GlobalString("cluster"))
	if err != nil {
		fmt.Printf("Could not obtain cluster state: %v\n", err)
		os.Exit(2)
	}
	_, ok := cs.Members[nn]
	if !ok {
		fmt.Printf("node '%s' does not exist\n", nn)
		os.Exit(1)
	}
	_, err = cc.Put(context.Background(), fmt.Sprintf("%s/x/m/%s/rpref", c.GlobalString("cluster"), nn), strconv.FormatFloat(rpref, 'f', 4, 64))
	if err != nil {
		fmt.Printf("Could not set node rpref: %v\n", err)
		os.Exit(2)
	}
	return nil
}
