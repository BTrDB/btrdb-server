package mprovider

import (
	"context"
	"fmt"
	"strings"

	"github.com/BTrDB/btrdb-server/bte"
	"github.com/coreos/etcd/clientv3"
)

const keyUsageBatchSize = 500

func (em *etcdMetadataProvider) GetKeyUsage(ctx context.Context, collectionPrefix string) (map[string]int, map[string]int, bte.BTE) {
	tagpath := fmt.Sprintf("%s/t/", em.pfx)
	annpath := fmt.Sprintf("%s/a/", em.pfx)
	rvann := make(map[string]int)
	rvtag := make(map[string]int)
	tagStart := tagpath
	for {
		if ctx.Err() != nil {
			return nil, nil, bte.CtxE(ctx)
		}
		resp, err := em.ec.Get(context.Background(), tagStart,
			clientv3.WithRange(clientv3.GetPrefixRangeEnd(tagpath)),
			clientv3.WithKeysOnly(),
			clientv3.WithLimit(keyUsageBatchSize))
		if err != nil {
			return nil, nil, bte.ErrW(bte.EtcdFailure, "could not query", err)
		}
		if len(resp.Kvs) == 0 {
			break
		}
		for _, kv := range resp.Kvs {
			parts := strings.SplitN(string(kv.Key), "/", 4)
			k := parts[2]
			col := parts[3][:len(parts[3])-37]
			if !strings.HasPrefix(col, collectionPrefix) {
				continue
			}
			rvtag[k] += 1
		}
		tagStart = string(resp.Kvs[len(resp.Kvs)-1].Key) + "\000"
	}
	annStart := annpath
	for {
		if ctx.Err() != nil {
			return nil, nil, bte.CtxE(ctx)
		}
		resp, err := em.ec.Get(context.Background(), annStart,
			clientv3.WithRange(clientv3.GetPrefixRangeEnd(annpath)),
			clientv3.WithKeysOnly(),
			clientv3.WithLimit(keyUsageBatchSize))
		if err != nil {
			return nil, nil, bte.ErrW(bte.EtcdFailure, "could not query", err)
		}
		if len(resp.Kvs) == 0 {
			break
		}
		for _, kv := range resp.Kvs {
			parts := strings.SplitN(string(kv.Key), "/", 4)
			k := parts[2]
			col := parts[3][:len(parts[3])-37]
			if !strings.HasPrefix(col, collectionPrefix) {
				continue
			}
			rvann[k] += 1
		}
		annStart = string(resp.Kvs[len(resp.Kvs)-1].Key) + "\000"
	}
	return rvtag, rvann, nil
}
