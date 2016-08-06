package main

import (
	"os"

	"github.com/urfave/cli"
)

/*
 usage examples
 btrdb status
  -> show cluster map
 btrdb --endpoints ip,ip,ip <cmd> --cluster <prefix>
 btrdb down <nodename>
 btrdb out <nodename>
 btrdb in <nodename>
 btrdb up <nodename>
 btrdb rm <nodename>
 btrdb weight <nodename> <newvalue>
 btrdb rpref <nodename> <newvalue>
*/

func main() {
	app := cli.NewApp()
	app.Name = "btrdb"
	app.Usage = "control a BTrDB cluster"
	app.Flags = []cli.Flag{
		cli.StringSliceFlag{
			Name:   "endpoint, e",
			Usage:  "add an etcd endpoint",
			Value:  &cli.StringSlice{"http://127.0.0.1:2379"},
			EnvVar: "BTRDB_ENDPOINT",
		},
		cli.StringFlag{
			Name:   "cluster, c",
			Usage:  "set the cluster name",
			Value:  "btrdb",
			EnvVar: "BTRDB_CLUSTER_NAME",
		},
	}
	app.Commands = []cli.Command{
		{
			Name:   "status",
			Usage:  "show the cluster status",
			Action: cli.ActionFunc(actionStatus),
		},
		{
			Name:   "disable",
			Action: cli.ActionFunc(actionDisable),
			Flags: []cli.Flag{
				cli.BoolFlag{Name: "force"},
			},
		},
		{
			Name:   "out",
			Action: cli.ActionFunc(actionOut),
		},
		{
			Name:   "enable",
			Action: cli.ActionFunc(actionEnable),
		},
		{
			Name:   "in",
			Action: cli.ActionFunc(actionIn),
		},
		{
			Name:   "rm",
			Action: cli.ActionFunc(actionRm),
		},
		{
			Name:   "weight",
			Action: cli.ActionFunc(actionWeight),
		},
		{
			Name:   "rpref",
			Action: cli.ActionFunc(actionRpref),
		},
	}
	app.Run(os.Args)
}
