// Copyright (c) 2021 Michael Andersen
// Copyright (c) 2021 Regents of the University Of California
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

package configprovider

import (
	"testing"
	"time"

	client "github.com/coreos/etcd/clientv3"

	"context"
)

func defaultConfig() *FileConfig {
	rv := FileConfig{}
	rv.Cluster.Prefix = "clustertest"
	rv.Cluster.EtcdEndpoint = []string{"http://127.0.0.1:2379"}
	rv.Cluster.Enabled = true
	return &rv
}
func getclient() *client.Client {
	rv, err := client.New(client.Config{
		Endpoints:   []string{"http://127.0.0.1:2379"},
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	return rv
}
func TestConfigCreate(t *testing.T) {
	fcfg := defaultConfig()
	_, err := LoadEtcdConfig(fcfg, "test1")
	if err != nil {
		t.FailNow()
	}
}
func TestActiveKey(t *testing.T) {
	ec := getclient()
	resp, err := ec.Get(context.TODO(), "clustertest/x/m/test2/active")
	if err != nil || resp.Count != 0 {
		t.Errorf("active key existed at start of test: %v %v", err, resp.Count)
	}
	fcfg := defaultConfig()
	cfg, err := LoadEtcdConfig(fcfg, "test2")
	if err != nil {
		t.Errorf("could not load config: %v", err)
	}
	resp, err = cfg.(*etcdconfig).eclient.Get(context.TODO(), "clustertest/x/m/test2/active")
	if err != nil || resp.Count != 1 {
		t.Errorf("active key did not exist after creating config: %v", err)
	}
	cfg.(ClusterConfiguration).Fault("deliberate")
	resp, err = cfg.(*etcdconfig).eclient.Get(context.TODO(), "clustertest/x/m/test2/active")
	if err != nil || resp.Count != 0 {
		t.Errorf("active key existed after fault: %v", err)
	}
}
func TestErrorOnSameNodename(t *testing.T) {
	fcfg := defaultConfig()
	_, err := LoadEtcdConfig(fcfg, "test3")
	if err != nil {
		t.Errorf("unexpected: %v", err)
	}
	_, err = LoadEtcdConfig(fcfg, "test3")
	if err == nil {
		t.Errorf("Should have got error, didn't")
	}
}
func TestLeaderElectedSolo(t *testing.T) {
	fcfg := defaultConfig()
	c1, err := LoadEtcdConfig(fcfg, "test4.1")
	if err != nil {
		t.Errorf("unexpected: %v", err)
	}
	_ = c1
	time.Sleep(10 * time.Second)
}
func TestLeaderElectedSoloEviction(t *testing.T) {
	fcfg := defaultConfig()
	c1, err := LoadEtcdConfig(fcfg, "test4.1")
	if err != nil {
		t.Errorf("unexpected: %v", err)
	}
	time.Sleep(1 * time.Second)
	c1.(ClusterConfiguration).Fault("deliberate")
	c2, err := LoadEtcdConfig(fcfg, "test4.2")
	if err != nil {
		t.Errorf("unexpected: %v", err)
	}
	_ = c2
	time.Sleep(40 * time.Second)
}
func TestLeaderConcurrent(t *testing.T) {
	fcfg := defaultConfig()
	_, err := LoadEtcdConfig(fcfg, "test5.1")
	if err != nil {
		t.Errorf("unexpected: %v", err)
	}
	time.Sleep(1 * time.Second)
	_, err = LoadEtcdConfig(fcfg, "test5.2")
	if err != nil {
		t.Errorf("unexpected: %v", err)
	}
	time.Sleep(1 * time.Second)
	_, err = LoadEtcdConfig(fcfg, "test5.3")
	if err != nil {
		t.Errorf("unexpected: %v", err)
	}
	time.Sleep(1 * time.Second)
	_, err = LoadEtcdConfig(fcfg, "test5.4")
	if err != nil {
		t.Errorf("unexpected: %v", err)
	}
	for {
		time.Sleep(1 * time.Second)
	}
}
