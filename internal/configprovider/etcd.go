package configprovider

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"context"

	"github.com/BTrDB/btrdb-server/internal/rez"
	client "github.com/coreos/etcd/clientv3"
	logging "github.com/op/go-logging"
)

var log *logging.Logger

func init() {
	log = logging.MustGetLogger("log")
}

type etcdconfig struct {
	eclient    *client.Client
	fileconfig Configuration
	nodename   string
	cman
	//Cached values
	cachedMaxPoints   int
	cachedMaxInterval int
}

//The file config is loaded first, and used to bootstrap etcd if requred
func LoadEtcdConfig(cfg Configuration, overrideNodename string) (Configuration, error) {
	rv := &etcdconfig{fileconfig: cfg}
	var err error
	nodenameprefix, _ := os.Hostname()

	fmt.Printf("Connecting to ETCD with %d endpoints. \nEPZ:(%#v)\n",
		len(cfg.ClusterEtcdEndpoints()), cfg.ClusterEtcdEndpoints())
	rv.eclient, err = client.New(client.Config{
		Endpoints:   cfg.ClusterEtcdEndpoints(),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Panicf("Could not create etcd client: %v", err)
	}

	index := 0
	nodename := overrideNodename
	if overrideNodename == "" {
		for {
			nodename = fmt.Sprintf("%s-%03d", nodenameprefix, index)
			gr, err := rv.eclient.Get(context.Background(), fmt.Sprintf("%s/n/%s/cephConf", cfg.ClusterPrefix(), nodename))
			if err != nil {
				log.Panicf("Could not use etcd client: %v", err)
			}
			if gr.Count == 0 {
				break
			}
			index++
		}
	}
	rv.nodename = nodename
	pk := func(k, v string, global bool) {
		path := fmt.Sprintf("%s/n/%s/%s", cfg.ClusterPrefix(), rv.nodename, k)
		if global {
			path = fmt.Sprintf("%s/g/%s", cfg.ClusterPrefix(), k)
		}
		_, err = rv.eclient.Put(rv.defctx(), path, v)
		if err != nil {
			log.Panicf("etcd error: %v", err)
		}
	}
	resp, err := rv.eclient.Get(rv.defctx(), fmt.Sprintf("%s/g", cfg.ClusterPrefix()), client.WithPrefix())
	if err != nil {
		log.Panicf("etcd error: %v", err)
	}
	if resp.Count == 0 {
		log.Warning("No global etcd config found, bootstrapping")

		//globals
		pk("cephDataPool", cfg.StorageCephDataPool(), true)
		pk("cephHotPool", cfg.StorageCephHotPool(), true)
		defaultTunables := rez.DefaultResourceTunables()
		for _, tunable := range defaultTunables {
			pk("tune/"+tunable[0], tunable[1], true)
		}
	}

	resp, err = rv.eclient.Get(rv.defctx(), fmt.Sprintf("%s/n/%s", cfg.ClusterPrefix(), rv.nodename), client.WithPrefix())
	if err != nil {
		log.Panicf("etcd error: %v", err)
	}
	if resp.Count == 0 {
		log.Warningf("No etcd config for this node (%s) found, bootstrapping", rv.nodename)
		//node default
		pk("cephConf", cfg.StorageCephConf(), false)
		pk("httpEnabled", strconv.FormatBool(cfg.HttpEnabled()), false)
		pk("httpListen", cfg.HttpListen(), false)

		pk("grpcEnabled", strconv.FormatBool(cfg.GRPCEnabled()), false)
		pk("grpcListen", cfg.GRPCListen(), false)

		pk("blockCache", strconv.FormatInt(int64(cfg.BlockCache()), 10), false)
		pk("radosReadCache", strconv.FormatInt(int64(cfg.RadosReadCache()), 10), false)
		pk("radosWriteCache", strconv.FormatInt(int64(cfg.RadosWriteCache()), 10), false)
		pk("coalesceMaxPoints", strconv.FormatInt(int64(cfg.CoalesceMaxPoints()), 10), false)
		pk("coalesceMaxInterval", strconv.FormatInt(int64(cfg.CoalesceMaxInterval()), 10), false)
	}
	//These parameters actually change because they are populated by the pod. Set them
	//each time
	pk("grpcAdvertise", strings.Join(cfg.GRPCAdvertise(), ";"), false)
	pk("httpAdvertise", strings.Join(cfg.HttpAdvertise(), ";"), false)

	//It is convenient to adjust this by changing the startup config:
	//bit of a hack though
	pk("blockCache", strconv.FormatInt(int64(cfg.BlockCache()), 10), false)
	pk("cephDataPool", cfg.StorageCephDataPool(), true)
	pk("cephHotPool", cfg.StorageCephHotPool(), true)

	return rv, nil
}
func LoadPoolNames(ctx context.Context, cl *client.Client, pfx string) (cold string, hot string, err error) {
	gk := func(k string) string {
		resp, err := cl.Get(ctx, fmt.Sprintf("%s/g/%s", pfx, k))
		if err != nil {
			log.Panicf("etcd error: %v", err)
		}
		if resp.Count != 1 {
			log.Panicf("expected one key, got %d", resp.Count)
		}
		return string(resp.Kvs[0].Value)
	}
	return gk("cephDataPool"), gk("cephHotPool"), nil
}
func (c *etcdconfig) BeginClusterDaemons() {
	err := c.cmanloop()
	if err != nil {
		c.Fault("Got top level error: %v", err)
		panic(err)
	}
}

func (c *etcdconfig) GetEtcdClient() *client.Client {
	return c.eclient
}
func (c *etcdconfig) NodeName() string {
	return c.nodename
}
func (c *etcdconfig) WatchTunable(name string, onchange func(v string)) error {
	path := fmt.Sprintf("%s/g/%s", c.ClusterPrefix(), "tune/"+name)

	rch := c.eclient.Watch(context.Background(), path)
	go func() {
		for wresp := range rch {
			for _, ev := range wresp.Events {
				onchange(string(ev.Kv.Value))
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	resp, err := c.eclient.Get(ctx, path)
	cancel()
	if err != nil {
		log.Panicf("etcd error: %v", err)
	}
	if len(resp.Kvs) != 1 {
		log.Panicf("tunable missing? %#v", resp.Kvs)
	}
	val := resp.Kvs[0].Value
	onchange(string(val))
	return nil
}

func (c *etcdconfig) stringNodeKey(key string) string {
	resp, err := c.eclient.Get(c.defctx(), fmt.Sprintf("%s/n/%s/%s", c.ClusterPrefix(), c.nodename, key))
	if err != nil {
		log.Panicf("etcd error: %v", err)
	}
	if resp.Count != 1 {
		log.Panicf("expected one key, got %d", resp.Count)
	}
	return string(resp.Kvs[0].Value)
}
func (c *etcdconfig) stringPeerNodeKey(nodename, key string) (string, error) {
	resp, err := c.eclient.Get(c.defctx(), fmt.Sprintf("%s/n/%s/%s", c.ClusterPrefix(), nodename, key))
	if err != nil {
		return "", err
	}
	if resp.Count != 1 {
		return "", fmt.Errorf("got %d values for key", resp.Count)
	}
	return string(resp.Kvs[0].Value), nil
}
func (c *etcdconfig) stringGlobalKey(key string) string {
	resp, err := c.eclient.Get(c.defctx(), fmt.Sprintf("%s/g/%s", c.ClusterPrefix(), key))
	if err != nil {
		log.Panicf("etcd error: %v", err)
	}
	if resp.Count != 1 {
		log.Panicf("expected one key, got %d", resp.Count)
	}
	return string(resp.Kvs[0].Value)
}
func (c *etcdconfig) defctx() context.Context {
	rv, _ := context.WithTimeout(context.Background(), 15*time.Second)
	return rv
}
func (c *etcdconfig) ClusterEnabled() bool {
	return true
}
func (c *etcdconfig) ClusterPrefix() string {
	return c.fileconfig.ClusterPrefix()
}
func (c *etcdconfig) ClusterEtcdEndpoints() []string {
	return c.fileconfig.ClusterEtcdEndpoints()
}
func (c *etcdconfig) StorageCephConf() string {
	return c.stringNodeKey("cephConf")
}
func (c *etcdconfig) StorageFilepath() string {
	panic("why on earth would you call this?")
}
func (c *etcdconfig) StorageCephDataPool() string {
	return c.stringGlobalKey("cephDataPool")
}
func (c *etcdconfig) StorageCephHotPool() string {
	return c.stringGlobalKey("cephHotPool")
}
func (c *etcdconfig) HttpEnabled() bool {
	return c.stringNodeKey("httpEnabled") == "true"
}
func (c *etcdconfig) HttpListen() string {
	return c.stringNodeKey("httpListen")
}
func (c *etcdconfig) HttpAdvertise() []string {
	j := c.stringNodeKey("httpAdvertise")
	if j == "" {
		return nil
	}
	return strings.Split(j, ";")
}
func (c *etcdconfig) GRPCEnabled() bool {
	return c.stringNodeKey("grpcEnabled") == "true"
}
func (c *etcdconfig) GRPCListen() string {
	return c.stringNodeKey("grpcListen")
}
func (c *etcdconfig) GRPCAdvertise() []string {
	j := c.stringNodeKey("grpcAdvertise")
	if j == "" {
		return nil
	}
	return strings.Split(j, ";")
}

func (c *etcdconfig) BlockCache() int {
	rv, err := strconv.Atoi(c.stringNodeKey("blockCache"))
	if err != nil {
		log.Panicf("could not decode block cache size from etcd: %v", err)
	}
	return rv
}
func (c *etcdconfig) RadosReadCache() int {
	rv, err := strconv.Atoi(c.stringNodeKey("radosReadCache"))
	if err != nil {
		log.Panicf("could not decode rados read cache size from etcd: %v", err)
	}
	return rv
}
func (c *etcdconfig) RadosWriteCache() int {
	rv, err := strconv.Atoi(c.stringNodeKey("radosWriteCache"))
	if err != nil {
		log.Panicf("could not decode rados write cache size from etcd: %v", err)
	}
	return rv
}
func (c *etcdconfig) CoalesceMaxPoints() int {
	if c.cachedMaxPoints == 0 {
		rv, err := strconv.Atoi(c.stringNodeKey("coalesceMaxPoints"))
		if err != nil {
			log.Panicf("could not decode coalesce max points from etcd: %v", err)
		}
		c.cachedMaxPoints = rv
	}
	return c.cachedMaxPoints
}
func (c *etcdconfig) CoalesceMaxInterval() int {
	if c.cachedMaxInterval == 0 {
		rv, err := strconv.Atoi(c.stringNodeKey("coalesceMaxInterval"))
		if err != nil {
			log.Panicf("could not decode coalesce max interval from etcd: %v", err)
		}
		c.cachedMaxInterval = rv
	}
	return c.cachedMaxInterval
}

func (c *etcdconfig) PeerHTTPAdvertise(nodename string) ([]string, error) {
	rv, err := c.stringPeerNodeKey(nodename, "httpAdvertise")
	if err != nil {
		return nil, err
	}
	if rv == "" {
		return []string{}, nil
	}
	return strings.Split(rv, ";"), nil
}
func (c *etcdconfig) PeerGRPCAdvertise(nodename string) ([]string, error) {
	rv, err := c.stringPeerNodeKey(nodename, "grpcAdvertise")
	if err != nil {
		return nil, err
	}
	if rv == "" {
		return []string{}, nil
	}
	return strings.Split(rv, ";"), nil
}
