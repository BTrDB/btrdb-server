package configprovider

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"golang.org/x/net/context"

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
	//Cached values
}

//The file config is loaded first, and used to bootstrap etcd if requred
func LoadEtcdConfig(cfg Configuration) (Configuration, error) {
	rv := &etcdconfig{fileconfig: cfg}
	var err error
	rv.nodename, err = os.Hostname()
	rv.eclient, err = client.New(client.Config{
		Endpoints:   cfg.ClusterEtcdEndpoints(),
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		log.Panicf("Could not create etcd client: %v")
	}
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
	resp, err := rv.eclient.Get(rv.defctx(), fmt.Sprintf("%s/g", cfg.ClusterPrefix()))
	if err != nil {
		log.Panicf("etcd error: %v", err)
	}
	if resp.Count == 0 {
		log.Info("No global etcd config found, bootstrapping")

		//globals
		pk("cephDataPool", cfg.StorageCephDataPool(), true)
		pk("cephHotPool", cfg.StorageCephHotPool(), true)
		pk("mongoServer", cfg.MongoServer(), true)
		pk("mongoCollection", cfg.MongoCollection(), true)
	}

	resp, err = rv.eclient.Get(rv.defctx(), fmt.Sprintf("%s/n/%s", cfg.ClusterPrefix(), rv.nodename))
	if err != nil {
		log.Panicf("etcd error: %v", err)
	}
	if resp.Count == 0 {
		log.Info("No etcd config for this node found, bootstrapping")
		//node default
		pk("cephConf", cfg.StorageCephConf(), false)
		pk("httpEnabled", strconv.FormatBool(cfg.HttpEnabled()), false)
		pk("httpPort", strconv.FormatInt(int64(cfg.HttpPort()), 10), false)
		pk("httpAddress", cfg.HttpAddress(), false)
		pk("capnpEnabled", strconv.FormatBool(cfg.CapnpEnabled()), false)
		pk("capnpPort", strconv.FormatInt(int64(cfg.CapnpPort()), 10), false)
		pk("capnpAddress", cfg.CapnpAddress(), false)
		pk("blockCache", strconv.FormatInt(int64(cfg.BlockCache()), 10), false)
		pk("radosReadCache", strconv.FormatInt(int64(cfg.RadosReadCache()), 10), false)
		pk("radosWriteCache", strconv.FormatInt(int64(cfg.RadosWriteCache()), 10), false)
		pk("coalesceMaxPoints", strconv.FormatInt(int64(cfg.CoalesceMaxPoints()), 10), false)
		pk("coalesceMaxInterval", strconv.FormatInt(int64(cfg.CoalesceMaxInterval()), 10), false)
		//
		// resp, err = rv.eclient.Get(rv.defctx(), fmt.Sprintf("%s/n/default", cfg.ClusterPrefix()), client.WithPrefix())
		// if err != nil {
		// 	log.Panicf("etcd error: %v", err)
		// }
		// if resp.Count == 0 {
		// 	log.Panicf("We expected the default config to exist?")
		// }
		// for _, kv := range resp.Kvs {
		// 	kkz := strings.Split(string(kv.Key), "/")
		// 	kk := kkz[len(kkz)-1]
		// 	log.Infof("loading default %s=%s", kk, kv.Value)
		// 	_, err := rv.eclient.Put(rv.defctx(), fmt.Sprintf("%s/n/%s/%s", rv.ClusterPrefix(), rv.nodename, kk), string(kv.Value))
		// 	if err != nil {
		// 		log.Panicf("etcd error: %v", err)
		// 	}
		// }
	}
	return rv, nil
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
	rv, _ := context.WithTimeout(context.Background(), 2*time.Second)
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
func (c *etcdconfig) HttpPort() int {
	rv, err := strconv.Atoi(c.stringNodeKey("httpPort"))
	if err != nil {
		log.Panicf("could not decode http port from etcd: %v", err)
	}
	return rv
}
func (c *etcdconfig) HttpAddress() string {
	return c.stringNodeKey("httpAddress")
}
func (c *etcdconfig) CapnpEnabled() bool {
	return c.stringNodeKey("capnpEnabled") == "true"
}
func (c *etcdconfig) CapnpPort() int {
	rv, err := strconv.Atoi(c.stringNodeKey("capnpPort"))
	if err != nil {
		log.Panicf("could not decode capnp port from etcd: %v", err)
	}
	return rv
}
func (c *etcdconfig) CapnpAddress() string {
	return c.stringNodeKey("capnpAddress")
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
	rv, err := strconv.Atoi(c.stringNodeKey("coalesceMaxPoints"))
	if err != nil {
		log.Panicf("could not decode coalesce max points from etcd: %v", err)
	}
	return rv
}
func (c *etcdconfig) CoalesceMaxInterval() int {
	rv, err := strconv.Atoi(c.stringNodeKey("coalesceMaxInterval"))
	if err != nil {
		log.Panicf("could not decode coalesce max interval from etcd: %v", err)
	}
	return rv
}
func (c *etcdconfig) MongoServer() string {
	return c.stringGlobalKey("mongoServer")
}
func (c *etcdconfig) MongoCollection() string {
	return c.stringGlobalKey("mongoCollection")
}
