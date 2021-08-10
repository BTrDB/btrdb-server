// Copyright (c) 2021 Michael Andersen
// Copyright (c) 2021 Regents of the University Of California
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

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
	app.Usage = "The BTrDB toolkit"
	endpoint_flag := cli.StringSliceFlag{
		Name:   "endpoint, e",
		Usage:  "BTrDB v4 endpoint",
		Value:  &cli.StringSlice{"127.0.0.1:4410"},
		EnvVar: "BTRDB_ENDPOINT",
	}
	legacy_endpoint_flag := cli.StringFlag{
		Name:   "legacy, l",
		Usage:  "BTrDB v3 server",
		Value:  "127.0.0.1:4410",
		EnvVar: "BTRDB_LEGACY",
	}
	_ = endpoint_flag
	_ = legacy_endpoint_flag
	app.Flags = []cli.Flag{
		cli.StringSliceFlag{
			Name:   "etcd",
			Usage:  "add an etcd endpoint",
			Value:  &cli.StringSlice{"http://127.0.0.1:2379"},
			EnvVar: "BTRDB_ETCD_ENDPOINT",
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
			Name:        "copy",
			Usage:       "copy data between streams",
			Description: "no really, there is a lot of dooing to be done",
			Category:    "data manipulation",
			Action:      cli.ActionFunc(actionCopy),
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "v3src",
					Usage: "ip:port of the v3 src server",
				},
				cli.StringFlag{
					Name:  "src",
					Usage: "ip:port of the v4 src server",
				},
				cli.StringFlag{
					Name:  "v3dst",
					Usage: "ip:port of the v3 dst server",
				},
				cli.StringFlag{
					Name:  "dst",
					Usage: "ip:port of the v4 dst server",
				},
				cli.StringFlag{
					Name:  "uuid",
					Usage: "the source uuid",
				},
				cli.StringFlag{
					Name:  "dstuuid",
					Usage: "the destination uuid or 'same'",
					Value: "same",
				},
				cli.StringFlag{
					Name:  "after",
					Usage: "start date in RFC3339 eg 2006-01-02T15:04:05Z07:00",
				},
				cli.StringFlag{
					Name:  "before",
					Usage: "end date in RFC3339 eg 2006-01-02T15:04:05Z07:00",
				},
				cli.BoolFlag{
					Name:  "delete",
					Usage: "delete range in dst before copy",
				},
			},
		},
	}
	app.Commands = append(app.Commands, ClusterAdminCommands...)

	app.Run(os.Args)
}
