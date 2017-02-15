package cliplugin

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/SoftwareDefinedBuildings/btrdb/internal/configprovider"
	etcd "github.com/coreos/etcd/clientv3"
	"github.com/immesys/smartgridstore/admincli"
)

//commands
//info - print MASH
//rpref
//reweight
//down
//up
//out
//in
//limits
// show
// set
//showlimits

//This will need to be changed
const clusterPrefix = "btrdb"

type btrdbCLI struct {
	c *etcd.Client
}

func NewBTrDBCLI(c *etcd.Client) admincli.CLIModule {

	cl := &btrdbCLI{c: c}
	return &admincli.GenericCLIModule{
		MName:  "btrdb",
		MHint:  "tune the BTrDB cluster",
		MUsage: "",
		MChildren: []admincli.CLIModule{
			&admincli.GenericCLIModule{
				MName:     "status",
				MHint:     "print cluster state",
				MUsage:    " ",
				MRun:      cl.status,
				MRunnable: true,
			},
			&admincli.GenericCLIModule{
				MName:     "disable",
				MHint:     "disable a member",
				MUsage:    " nodename",
				MRun:      cl.disable,
				MRunnable: true,
			},
			&admincli.GenericCLIModule{
				MName:     "out",
				MHint:     "disallow member participation",
				MUsage:    " nodename",
				MRun:      cl.out,
				MRunnable: true,
			},
			&admincli.GenericCLIModule{
				MName:     "enable",
				MHint:     "enable a member",
				MUsage:    " nodename",
				MRun:      cl.enable,
				MRunnable: true,
			},
			&admincli.GenericCLIModule{
				MName:     "in",
				MHint:     "allow member participation",
				MUsage:    " nodename",
				MRun:      cl.in,
				MRunnable: true,
			},
			&admincli.GenericCLIModule{
				MName:     "rm",
				MHint:     "remove a member",
				MUsage:    " nodename",
				MRun:      cl.rm,
				MRunnable: true,
			},
			&admincli.GenericCLIModule{
				MName:     "weight",
				MHint:     "set a member weight",
				MUsage:    " nodename weight",
				MRun:      cl.weight,
				MRunnable: true,
			},
			&admincli.GenericCLIModule{
				MName:     "rpref",
				MHint:     "set a member read preference",
				MUsage:    " nodename rpref",
				MRun:      cl.rpref,
				MRunnable: true,
			},
			&admincli.GenericCLIModule{
				MName:  "limits",
				MHint:  "manage resource limits",
				MUsage: "",
				MChildren: []admincli.CLIModule{
					&admincli.GenericCLIModule{
						MName:     "show",
						MHint:     "show current limits",
						MUsage:    "",
						MRun:      cl.showlimits,
						MRunnable: true,
					},
					&admincli.GenericCLIModule{
						MName:     "set",
						MHint:     "set limit",
						MUsage:    " type poolsize queuesize",
						MRun:      cl.setlimit,
						MRunnable: true,
					},
				},
			},
		},
	}
}

func (b *btrdbCLI) status(ctx context.Context, out io.Writer, args ...string) bool {
	cs, err := configprovider.QueryClusterState(ctx, b.c, clusterPrefix)
	if err != nil {
		fmt.Fprintf(out, "could not obtain cluster state: %v\n", err)
	} else {
		fmt.Fprintln(out, cs.String())
	}
	return true
}

func (b *btrdbCLI) in(ctx context.Context, out io.Writer, args ...string) bool {
	if len(args) != 1 {
		return false
	}
	nn := args[0]
	if nn == "" {
		return false
	}
	cs, err := configprovider.QueryClusterState(ctx, b.c, clusterPrefix)
	if err != nil {
		fmt.Fprintf(out, "could not obtain cluster state: %v\n", err)
		return true
	}
	_, ok := cs.Members[nn]
	if !ok {
		fmt.Fprintf(out, "node '%s' does not exist\n", nn)
		return true
	}
	_, err = b.c.Put(ctx, fmt.Sprintf("%s/x/m/%s/in", clusterPrefix, nn), "true")
	if err != nil {
		fmt.Fprintf(out, "could not in node: %v\n", err)
	}
	return true
}

func (b *btrdbCLI) enable(ctx context.Context, out io.Writer, args ...string) bool {
	if len(args) != 1 {
		return false
	}
	nn := args[0]
	if nn == "" {
		return false
	}
	cs, err := configprovider.QueryClusterState(ctx, b.c, clusterPrefix)
	if err != nil {
		fmt.Fprintf(out, "could not obtain cluster state: %v\n", err)
		return true
	}
	_, ok := cs.Members[nn]
	if !ok {
		fmt.Fprintf(out, "node '%s' does not exist\n", nn)
		return true
	}
	_, err = b.c.Put(ctx, fmt.Sprintf("%s/x/m/%s/enabled", clusterPrefix, nn), "true")
	if err != nil {
		fmt.Fprintf(out, "could not enable node: %v\n", err)
	}
	return true
}

func (b *btrdbCLI) out(ctx context.Context, out io.Writer, args ...string) bool {
	if len(args) != 1 {
		return false
	}
	nn := args[0]
	if nn == "" {
		return false
	}
	cs, err := configprovider.QueryClusterState(ctx, b.c, clusterPrefix)
	if err != nil {
		fmt.Fprintf(out, "could not obtain cluster state: %v\n", err)
		return true
	}
	_, ok := cs.Members[nn]
	if !ok {
		fmt.Fprintf(out, "node '%s' does not exist\n", nn)
		return true
	}
	_, err = b.c.Put(ctx, fmt.Sprintf("%s/x/m/%s/in", clusterPrefix, nn), "false")
	if err != nil {
		fmt.Fprintf(out, "could not out node: %v\n", err)
	}
	return true
}

func (b *btrdbCLI) disable(ctx context.Context, out io.Writer, args ...string) bool {
	if len(args) != 1 {
		return false
	}
	nn := args[0]
	if nn == "" {
		return false
	}
	cs, err := configprovider.QueryClusterState(ctx, b.c, clusterPrefix)
	if err != nil {
		fmt.Fprintf(out, "could not obtain cluster state: %v\n", err)
		return true
	}
	m, ok := cs.Members[nn]
	if !ok {
		fmt.Fprintf(out, "node '%s' does not exist\n", nn)
		return true
	}
	if m.In {
		fmt.Fprintf(out, "cannot delete, node is IN\nplease OUT the node and wait for cluster to be Healthy\n")
		return true
	}
	_, err = b.c.Put(context.Background(), fmt.Sprintf("%s/x/m/%s/enabled", clusterPrefix, nn), "false")
	if err != nil {
		fmt.Fprintf(out, "could not disable node: %v\n", err)
	}
	return true
}

func (b *btrdbCLI) rm(ctx context.Context, out io.Writer, args ...string) bool {
	if len(args) != 1 {
		return false
	}
	nn := args[0]
	if nn == "" {
		return false
	}
	cs, err := configprovider.QueryClusterState(ctx, b.c, clusterPrefix)
	if err != nil {
		fmt.Fprintf(out, "could not obtain cluster state: %v\n", err)
		return true
	}
	m, ok := cs.Members[nn]
	if !ok {
		fmt.Fprintf(out, "node '%s' does not exist\n", nn)
		return true
	}
	if m.In {
		fmt.Fprintf(out, "cannot delete, node is IN\n")
		return true
	}
	if m.Enabled {
		fmt.Fprintf(out, "cannot delete, node is ENABLED\n")
		return true
	}
	_, err = b.c.Delete(ctx, fmt.Sprintf("%s/x/m/%s", clusterPrefix, nn), etcd.WithPrefix())
	if err != nil {
		fmt.Fprintf(out, "could not delete node: %v\n", err)
	}
	return true
}

func (b *btrdbCLI) weight(ctx context.Context, out io.Writer, args ...string) bool {
	if len(args) != 2 {
		return false
	}
	nn := args[0]
	if nn == "" {
		return false
	}
	weight, err := strconv.ParseUint(args[1], 10, 32)
	if err != nil {
		fmt.Fprintf(out, "bad weight, must be positive integer")
		return true
	}
	cs, err := configprovider.QueryClusterState(ctx, b.c, clusterPrefix)
	if err != nil {
		fmt.Fprintf(out, "could not obtain cluster state: %v\n", err)
		return true
	}
	_, ok := cs.Members[nn]
	if !ok {
		fmt.Fprintf(out, "node '%s' does not exist\n", nn)
		return true
	}
	_, err = b.c.Put(ctx, fmt.Sprintf("%s/x/m/%s/weight", clusterPrefix, nn), strconv.FormatUint(weight, 10))
	if err != nil {
		fmt.Fprintf(out, "could not set node weight: %v\n", err)
	}
	return true
}

func (b *btrdbCLI) rpref(ctx context.Context, out io.Writer, args ...string) bool {
	if len(args) != 2 {
		return false
	}
	nn := args[0]
	if nn == "" {
		return false
	}
	rpref, err := strconv.ParseFloat(args[1], 64)
	if err != nil || rpref < 0 {
		fmt.Fprintf(out, "bad rpref, must be a float >= 0")
		return true
	}
	cs, err := configprovider.QueryClusterState(context.Background(), b.c, clusterPrefix)
	if err != nil {
		fmt.Fprintf(out, "could not obtain cluster state: %v\n", err)
		return true
	}
	_, ok := cs.Members[nn]
	if !ok {
		fmt.Fprintf(out, "node '%s' does not exist\n", nn)
		return true
	}
	_, err = b.c.Put(ctx, fmt.Sprintf("%s/x/m/%s/rpref", clusterPrefix, nn), strconv.FormatFloat(rpref, 'f', 4, 64))
	if err != nil {
		fmt.Fprintf(out, "could not set node rpref: %v\n", err)
	}
	return true
}

func (b *btrdbCLI) showlimits(ctx context.Context, out io.Writer, args ...string) bool {
	path := fmt.Sprintf("%s/g/tune/", clusterPrefix)
	rv, err := b.c.Get(ctx, path, etcd.WithPrefix())
	if err != nil {
		fmt.Fprintf(out, "could not load limits: %v\n", err)
		return true
	}
	fmt.Fprintf(out, "%-20s %-6s %-6s\n", "resource", "pool", "queue")
	fmt.Fprintf(out, "----------------------------------\n")
	for _, e := range rv.Kvs {
		vals := strings.Split(string(e.Value), ",")
		kz := strings.Split(string(e.Key), "/")
		k := kz[len(kz)-1]
		fmt.Fprintf(out, "%-20s %-6s %-6s\n", k, vals[0], vals[1])
	}
	return true
}

func (b *btrdbCLI) setlimit(ctx context.Context, out io.Writer, args ...string) bool {
	//set limit pool queue
	if len(args) != 3 {
		return false
	}
	limit := args[0]
	pool, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		fmt.Fprintf(out, "could not parse pool size: %v\n", err)
		return true
	}
	queue, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		fmt.Fprintf(out, "could not parse queue size: %v\n", err)
		return true
	}
	if pool < 0 || queue < 0 {
		fmt.Fprintf(out, "limits must be >= zero:\n")
		return true
	}
	strval := fmt.Sprintf("%d,%d", pool, queue)
	key := fmt.Sprintf("%s/g/tune/%s", clusterPrefix, limit)
	rv, err := b.c.Txn(ctx).If(etcd.Compare(etcd.Version(key), ">", 0)).
		Then(etcd.OpPut(key, strval)).
		Commit()
	if err != nil || !rv.Succeeded {
		fmt.Fprintf(out, "could not set limit, correct name? (%v)\n", err)
		return true
	} else {
		fmt.Fprintf(out, "limit updated\n")
		return true
	}

}
