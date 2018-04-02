package cliplugin

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	btrdb "gopkg.in/BTrDB/btrdb.v4"

	"github.com/BTrDB/btrdb-server/bte"
	"github.com/BTrDB/btrdb-server/internal/cephprovider"
	"github.com/BTrDB/btrdb-server/internal/configprovider"
	"github.com/BTrDB/btrdb-server/internal/mprovider"
	"github.com/BTrDB/smartgridstore/admincli"
	"github.com/ceph/go-ceph/rados"
	etcd "github.com/coreos/etcd/clientv3"
	"github.com/pborman/uuid"
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
				MName:     "autoprune",
				MHint:     "disable and rm all down members",
				MUsage:    "",
				MRun:      cl.autoprune,
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
				MName:  "throttle",
				MHint:  "manage resource throttles",
				MUsage: "",
				MChildren: []admincli.CLIModule{
					&admincli.GenericCLIModule{
						MName:     "show",
						MHint:     "show current throttles",
						MUsage:    "",
						MRun:      cl.showlimits,
						MRunnable: true,
					},
					&admincli.GenericCLIModule{
						MName:     "set",
						MHint:     "set throttle",
						MUsage:    " type poolsize queuesize",
						MRun:      cl.setlimit,
						MRunnable: true,
					},
				},
			},
			&admincli.GenericCLIModule{
				MName:  "utils",
				MHint:  "list and delete streams",
				MUsage: "",
				MChildren: []admincli.CLIModule{
					&admincli.GenericCLIModule{
						MName:     "lookup",
						MHint:     "search by collection and tags/annotations",
						MUsage:    " collectionprefix [tag.<tag_name> <tagval>] [ann.<annotation_name> <annval>]",
						MRun:      cl.lookup,
						MRunnable: true,
					},
					&admincli.GenericCLIModule{
						MName:     "obliterate",
						MHint:     "completely remove a stream forever",
						MUsage:    " <uuid>",
						MRun:      cl.obliterate,
						MRunnable: true,
					},
					&admincli.GenericCLIModule{
						MName:     "obliterateprefix",
						MHint:     "completely remove a set of streams",
						MUsage:    " <collectionprefix>",
						MRun:      cl.obliteratePrefix,
						MRunnable: true,
					},
					&admincli.GenericCLIModule{
						MName:     "setann",
						MHint:     "set annotations on a stream",
						MUsage:    " <uuid> [annotation_name annotation_value]...",
						MRun:      cl.setann,
						MRunnable: true,
					},
					&admincli.GenericCLIModule{
						MName:     "unsetann",
						MHint:     "remove annotations on a stream",
						MUsage:    " <uuid> [annotation_name]...",
						MRun:      cl.unsetann,
						MRunnable: true,
					},
					&admincli.GenericCLIModule{
						MName:     "renameprefix",
						MHint:     "rename streams (not recommended)",
						MUsage:    " <oldprefix> <newprefix>",
						MRun:      cl.renamePrefix,
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

const lowestPoint int64 = -(12 << 56)

func (b *btrdbCLI) lookup(ctx context.Context, out io.Writer, args ...string) bool {
	if len(args) < 1 {
		return false
	}
	if len(args)%2 != 1 {
		return false
	}
	db, err := btrdb.Connect(ctx, btrdb.EndpointsFromEnv()...)
	if err != nil {
		fmt.Fprintf(out, "could not connect to BTrDB: %v\n", err)
		return true
	}
	prefix := args[0]
	tags := make(map[string]*string)
	anns := make(map[string]*string)
	for i := 1; i < len(args); i += 2 {
		k := args[i]
		v := args[i+1]
		if strings.HasPrefix(k, "tag.") {
			tags[strings.TrimPrefix(k, "tag.")] = &v
		} else if strings.HasPrefix(k, "ann.") {
			anns[strings.TrimPrefix(k, "ann.")] = &v
		} else {
			return false
		}
	}
	rv, err := db.LookupStreams(context.Background(), prefix, true, tags, anns)
	if err != nil {
		fmt.Fprintf(out, "could not query BTrDB: %v\n", err)
	}

	//Optimize for 120 character output
	heading := "Stream canonical uuid                Points    Collection               Tags & Annotations\n"
	fmt.Fprintf(out, heading)
	totalcount := uint64(0)
	totalstreams := 0
	colfmt := "%-36s %-9d %-24s %s\n"
	//         "118c49bc-763e-11e7-a20f-0cc47a738395 500000000 "
	for _, r := range rv {
		totalstreams++
		tags, err := r.Tags(ctx)
		if err != nil {
			fmt.Fprintf(out, "could not query tags: %v\n", err)
			return true
		}
		anns, _, err := r.Annotations(ctx)
		if err != nil {
			fmt.Fprintf(out, "could not query annotations: %v\n", err)
			return true
		}
		col, err := r.Collection(ctx)
		if err != nil {
			fmt.Fprintf(out, "could not query collection: %v\n", err)
			return true
		}
		csp, _, cerr := r.AlignedWindows(ctx, 0, (1<<61)-1, 61, 0)
		sv := <-csp
		err = <-cerr
		if err != nil {
			fmt.Fprintf(out, "could not count stream: %v\n", err)
			return true
		}
		atag := ""
		for tk, tv := range tags {
			atag += fmt.Sprintf("T(%q=%q) ", tk, tv)
		}
		for ak, av := range anns {
			atag += fmt.Sprintf("A(%q=%q) ", ak, av)
		}
		count := sv.Count
		totalcount += count
		fmt.Fprintf(out, colfmt, r.UUID().String(), count, col, atag)
	}
	fmt.Fprintf(out, "TOTAL: %d streams, %d points\n", totalstreams, totalcount)
	return true
}

func (b *btrdbCLI) setann(ctx context.Context, out io.Writer, args ...string) bool {
	if len(args) < 1 {
		return false
	}
	if len(args)%2 != 1 {
		return false
	}
	db, err := btrdb.Connect(ctx, btrdb.EndpointsFromEnv()...)
	if err != nil {
		fmt.Fprintf(out, "could not connect to BTrDB: %v\n", err)
		return true
	}
	uu := uuid.Parse(args[0])
	if uu == nil {
		fmt.Fprintf(out, "could not parse uuid %q", args[0])
		return true
	}
	s := db.StreamFromUUID(uu)
	_, aver, err := s.Annotations(ctx)
	if err != nil {
		fmt.Fprintf(out, "could not query BTrDB: %v\n", err)
	}
	changes := make(map[string]*string)
	for i := 1; i < len(args); i += 2 {
		k := args[i]
		v := args[i+1]
		changes[k] = &v
	}
	err = s.CompareAndSetAnnotation(ctx, aver, changes)
	if err != nil {
		fmt.Fprintf(out, "could not set annotations: %v\n", err)
	}
	return true
}

func (b *btrdbCLI) unsetann(ctx context.Context, out io.Writer, args ...string) bool {
	if len(args) < 1 {
		return false
	}
	db, err := btrdb.Connect(ctx, btrdb.EndpointsFromEnv()...)
	if err != nil {
		fmt.Fprintf(out, "could not connect to BTrDB: %v\n", err)
		return true
	}
	uu := uuid.Parse(args[0])
	if uu == nil {
		fmt.Fprintf(out, "could not parse uuid %q", args[0])
		return true
	}
	s := db.StreamFromUUID(uu)
	_, aver, err := s.Annotations(ctx)
	if err != nil {
		fmt.Fprintf(out, "could not query BTrDB: %v\n", err)
	}
	changes := make(map[string]*string)
	for i := 1; i < len(args); i += 1 {
		k := args[i]
		changes[k] = nil
	}
	err = s.CompareAndSetAnnotation(ctx, aver, changes)
	if err != nil {
		fmt.Fprintf(out, "could not set annotations: %v\n", err)
	}
	return true
}

func (b *btrdbCLI) obliterate(ctx context.Context, out io.Writer, args ...string) bool {
	if len(args) != 1 {
		return false
	}
	uu := uuid.Parse(args[0])
	if uu == nil {
		fmt.Fprintf(out, "could not parse uuid %q", args[0])
		return true
	}
	db, err := btrdb.Connect(ctx, btrdb.EndpointsFromEnv()...)
	if err != nil {
		fmt.Fprintf(out, "could not connect to BTrDB: %v\n", err)
		return true
	}
	s := db.StreamFromUUID(uu)
	col, err := s.Collection(ctx)
	if err != nil {
		fmt.Fprintf(out, "Could not lookup stream: %v\n", err)
		return true
	}
	err = s.Obliterate(ctx)
	if err != nil {
		fmt.Fprintf(out, "Could not obliterate stream: %v\n", err)
		return true
	}
	fmt.Fprintf(out, "Stream %q in collection %q has been obliterated", uu.String(), col)
	return true
}

func tagString(tags map[string]string) string {
	strs := []string{}
	sz := 1 //one extra for fun
	for k, v := range tags {
		sz += 2 + len(k) + len(v)
		strs = append(strs, fmt.Sprintf("%s\x00%s\x00", k, v))
	}
	sort.StringSlice(strs).Sort()
	ts := bytes.NewBuffer(make([]byte, 0, sz))
	for _, s := range strs {
		ts.WriteString(s)
	}
	return ts.String()
}

func (b *btrdbCLI) _renameStream(ctx context.Context, uuid []byte, oldcollection string, collection string, tags map[string]string, annotations map[string]string) bte.BTE {
	const etcdprefix = "btrdb"
	if tags == nil {
		tags = make(map[string]string)
	}
	if annotations == nil {
		annotations = make(map[string]string)
	}
	fr := &mprovider.FullRecord{
		Tags:       tags,
		Anns:       annotations,
		Collection: collection,
	}
	streamkey := fmt.Sprintf("%s/u/%s", etcdprefix, string(uuid))
	opz := []etcd.Op{}
	opz = append(opz, etcd.OpPut(streamkey, string(fr.Serialize())))
	for k, v := range tags {
		path := fmt.Sprintf("%s/t/%s/%s/%s", etcdprefix, k, collection, string(uuid))
		opz = append(opz, etcd.OpPut(path, v))
		oldpath := fmt.Sprintf("%s/t/%s/%s/%s", etcdprefix, k, oldcollection, string(uuid))
		opz = append(opz, etcd.OpDelete(oldpath))
	}
	for k, v := range annotations {
		path := fmt.Sprintf("%s/a/%s/%s/%s", etcdprefix, k, collection, string(uuid))
		opz = append(opz, etcd.OpPut(path, v))
		oldpath := fmt.Sprintf("%s/a/%s/%s/%s", etcdprefix, k, oldcollection, string(uuid))
		opz = append(opz, etcd.OpDelete(oldpath))
	}
	//Although this may exist, it is important to write to it again
	//because the delete code will transact on the version of this
	colpath := fmt.Sprintf("%s/c/%s/", etcdprefix, collection)
	opz = append(opz, etcd.OpPut(colpath, "NA"))
	tagstring := tagString(tags)
	tagstringpath := fmt.Sprintf("%s/s/%s/%s", etcdprefix, collection, tagstring)
	opz = append(opz, etcd.OpPut(tagstringpath, string(uuid)))
	oldtagstringpath := fmt.Sprintf("%s/s/%s/%s", etcdprefix, oldcollection, tagstring)
	opz = append(opz, etcd.OpDelete(oldtagstringpath))
	txr, err := b.c.Txn(ctx).
		If(etcd.Compare(etcd.Version(tagstringpath), "=", 0)).
		Then(opz...).
		Commit()
	if err != nil {
		return bte.ErrW(bte.EtcdFailure, "could not create stream", err)
	}
	if !txr.Succeeded {
		//Perhaps tagstring collided
		kv, err := b.c.Get(ctx, tagstringpath)
		if err != nil {
			return bte.ErrW(bte.EtcdFailure, "could not create stream", err)
		}
		if kv.Count != 0 {
			return bte.Err(bte.StreamExists, fmt.Sprintf("a stream already exists in that collection with identical tags"))
		}

		//Perhaps stream uuid exists, otherwise it was tombstone
		kv, err = b.c.Get(ctx, streamkey)
		if err != nil {
			return bte.ErrW(bte.EtcdFailure, "could not create stream", err)
		}
		if kv.Count == 0 {
			return bte.Err(bte.ReusedUUID, fmt.Sprintf("uuid has been used before with a (now deleted) stream"))
		} else {
			return bte.Err(bte.ReusedUUID, fmt.Sprintf("a stream already exists with uuid and a different collection or tags"))
		}
	}

	//Now we also need to potentiall delete the collection record
	colpath = fmt.Sprintf("%s/c/%s/", etcdprefix, oldcollection)
	ckv, err := b.c.Get(ctx, colpath)
	if err != nil {
		return bte.ErrW(bte.EtcdFailure, "could not delete stream", err)
	}
	if ckv.Count == 0 {
		//no need to delete col
		return nil
	}
	ver := ckv.Kvs[0].Version

	crprefix := fmt.Sprintf("%s/s/%s/", etcdprefix, oldcollection)
	kv, err := b.c.Get(ctx, crprefix, etcd.WithPrefix())
	if err != nil {
		return bte.ErrW(bte.EtcdFailure, "could not delete stream", err)
	}
	if kv.Count == 0 {
		//We need to delete the collection head
		_, err := b.c.Txn(ctx).
			If(etcd.Compare(etcd.Version(colpath), "=", ver)).
			Then(etcd.OpDelete(colpath)).
			Commit()
		if err != nil {
			return bte.ErrW(bte.EtcdFailure, "could not delete stream", err)
		}
	}
	return nil
}

func (b *btrdbCLI) renamePrefix(ctx context.Context, out io.Writer, args ...string) bool {
	if len(args) != 2 {
		return false
	}
	prefix := args[0]
	newprefix := args[1]
	db, err := btrdb.Connect(ctx, btrdb.EndpointsFromEnv()...)
	if err != nil {
		fmt.Fprintf(out, "could not connect to BTrDB: %v\n", err)
		return true
	}
	streamz, err := db.LookupStreams(ctx, prefix, true, nil, nil)
	if err != nil {
		fmt.Fprintf(out, "Could not lookup streams: %v\n", err)
		return true
	}
	fmt.Fprintf(out, "Renaming %d streams (v2)\n", len(streamz))
	for _, str := range streamz {
		oldcollection, err := str.Collection(ctx)
		if err != nil {
			fmt.Fprintf(out, "Could not lookup collection: %v\n", err)
			return true
		}
		newcollectionsuffix := strings.TrimPrefix(oldcollection, prefix)
		newcollection := newprefix + newcollectionsuffix
		tags, err := str.Tags(ctx)
		if err != nil {
			fmt.Fprintf(out, "Could not lookup tags: %v\n", err)
			return true
		}
		annotations, _, err := str.Annotations(ctx)
		if err != nil {
			fmt.Fprintf(out, "Could not lookup annotations: %v\n", err)
			return true
		}
		err = b._renameStream(ctx, str.UUID(), oldcollection, newcollection, tags, annotations)
		if err != nil {
			fmt.Fprintf(out, "Rename failed: %v\n", err)
			return true
		}
	}
	fmt.Fprintf(out, "all streams renamed. Good luck\n")
	return true
}
func (b *btrdbCLI) obliteratePrefix(ctx context.Context, out io.Writer, args ...string) bool {
	if len(args) != 1 {
		return false
	}
	prefix := args[0]
	db, err := btrdb.Connect(ctx, btrdb.EndpointsFromEnv()...)
	if err != nil {
		fmt.Fprintf(out, "could not connect to BTrDB: %v\n", err)
		return true
	}
	streamz, err := db.LookupStreams(ctx, prefix, true, nil, nil)
	if err != nil {
		fmt.Fprintf(out, "Could not lookup streams: %v\n", err)
		return true
	}
	fmt.Fprintf(out, "Obliterating %d streams: \n", len(streamz))
	heading := "Stream canonical uuid                Collection               Tags & Annotations\n"
	fmt.Fprintf(out, heading)
	for _, s := range streamz {
		if ctx.Err() != nil {
			fmt.Fprintf(out, "Abort: context error: %v\n", ctx.Err())
			return true
		}
		colfmt := "%-36s %-24s %s\n"
		tags, err := s.Tags(ctx)
		if err != nil {
			fmt.Fprintf(out, "could not query tags: %v\n", err)
			return true
		}
		anns, _, err := s.Annotations(ctx)
		if err != nil {
			fmt.Fprintf(out, "could not query annotations: %v\n", err)
			return true
		}
		col, err := s.Collection(ctx)
		if err != nil {
			fmt.Fprintf(out, "could not query collection: %v\n", err)
			return true
		}
		atag := ""
		for tk, tv := range tags {
			atag += fmt.Sprintf("T(%q=%q) ", tk, tv)
		}
		for ak, av := range anns {
			atag += fmt.Sprintf("A(%q=%q) ", ak, av)
		}
		fmt.Fprintf(out, colfmt, s.UUID().String(), col, atag)
		err = s.Obliterate(ctx)
		if err != nil {
			fmt.Fprintf(out, "Could not obliterate stream: %v\n", err)
			return true
		}
	}
	fmt.Fprintf(out, "A total of %d streams were obliterated\n", len(streamz))
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

func (b *btrdbCLI) autoprune(ctx context.Context, out io.Writer, args ...string) bool {
	cs, err := configprovider.QueryClusterState(ctx, b.c, clusterPrefix)
	if err != nil {
		fmt.Fprintf(out, "could not obtain cluster state: %v\n", err)
	}
	for _, mbr := range cs.Members {
		if mbr.IsIn() {
			fmt.Fprintf(out, "skipping IN member %s\n", mbr.Nodename)
			continue
		}
		fmt.Printf("OUT %s\n", mbr.Nodename)
		if !b.out(ctx, out, mbr.Nodename) {
			return false
		}
		fmt.Printf("DIS %s\n", mbr.Nodename)
		if !b.disable(ctx, out, mbr.Nodename) {
			return false
		}
		fmt.Printf("RM %s\n", mbr.Nodename)
		if !b.rm(ctx, out, mbr.Nodename) {
			return false
		}
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
	//Get the pool name
	_, hot, err := configprovider.LoadPoolNames(ctx, b.c, clusterPrefix)
	conn, _ := rados.NewConn()
	conn.ReadDefaultConfigFile()
	err = conn.Connect()
	if err != nil {
		panic(err)
	}
	defer conn.Shutdown()
	ioctx, err := conn.OpenIOContext(hot)
	ioctx.SetNamespace(cephprovider.CJournalProviderNamespace)
	err = cephprovider.ForgetAboutNode(ctx, ioctx, nn)
	if err != nil {
		panic(err)
	}
	ioctx.Destroy()
	//Tell the journal provider to forget the node
	//jp := cephprovider.NewJournalProvider(cfg configprovider.Configuration, ccfg configprovider.ClusterConfiguration) (jprovider.JournalProvider, bte.BTE) {
	_, err = b.c.Delete(ctx, fmt.Sprintf("%s/x/m/%s", clusterPrefix, nn), etcd.WithPrefix())
	if err != nil {
		fmt.Fprintf(out, "could not delete node: %v\n", err)
	}
	_, err = b.c.Delete(ctx, fmt.Sprintf("%s/n/%s", clusterPrefix, nn), etcd.WithPrefix())
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
