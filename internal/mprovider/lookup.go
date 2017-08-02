package mprovider

import (
	"bytes"
	"context"
	"fmt"

	"github.com/SoftwareDefinedBuildings/btrdb/bte"
	etcd "github.com/coreos/etcd/clientv3"
)

const cursorBufferSize = 3

type cursor struct {
	ctx          context.Context
	ec           *etcd.Client
	idx          int
	prefix       string
	startFrom    string
	desiredValue []byte
	optional     bool
	buffer       *etcd.GetResponse
	traversed    int
}

func (c *cursor) keyToHead(b []byte) []byte {
	if b == nil {
		return b
	}
	return b[len(c.prefix):]
}

func validateCollectionTagsAndAnns(collection string, tags map[string]*string, annotations map[string]*string) bte.BTE {
	if len(collection) > 0 && !isValidCollection(collection) {
		return bte.Err(bte.InvalidCollection, fmt.Sprintf("collection %q is invalid", collection))
	}
	for k, v := range tags {
		if !isValidTagKey(k) {
			return bte.Err(bte.InvalidTagKey, fmt.Sprintf("tag key %q is invalid", k))
		}
		if v != nil && !isValidTagValue(*v) {
			return bte.Err(bte.InvalidTagValue, fmt.Sprintf("tag value for key %q is invalid", k))
		}
	}
	for k, v := range annotations {
		if !isValidAnnKey(k) {
			return bte.Err(bte.InvalidTagKey, fmt.Sprintf("annotation key %q is invalid", k))
		}
		if v != nil && !isValidAnnotationValue(*v) {
			return bte.Err(bte.InvalidTagValue, fmt.Sprintf("annotation value for key %q is invalid", k))
		}
	}
	return nil
}

//advance the cursor until the head is greater than or equal to the given uuid
func (c *cursor) advanceUntilGTE(uu []byte) (gt bool, head []byte, finished bool, err bte.BTE) {
	for {
		head, fin, err := c.head()
		if err != nil || fin {
			return false, nil, fin, err
		}
		cmp := bytes.Compare(head, uu)
		if cmp >= 0 {
			return cmp > 0, head, false, nil
		} else {
			_, fin, err := c.pop()
			if err != nil || fin {
				return false, nil, fin, err
			}
		}
	}
}
func (c *cursor) head() (head []byte, finished bool, err bte.BTE) {
	for {
		if c.idx < 0 || c.idx >= len(c.buffer.Kvs) {
			return c.pop()
		}
		rv := c.buffer.Kvs[c.idx]
		return c.keyToHead(rv.Key), false, nil
	}
}
func (c *cursor) pop() (head []byte, finished bool, err bte.BTE) {
	for {
		head, value, fin, err := c.rawpop()
		if err != nil || fin {
			return nil, fin, err
		}
		if c.optional || bytes.Equal(value, c.desiredValue) {
			return head, false, nil
		}
	}
}
func (c *cursor) rawpop() (head []byte, value []byte, finished bool, err bte.BTE) {

	if c.buffer == nil {
		//We need a new buffer
		c.refill()
		//Normally we ignore first entry in buffer, but this time
		//its relevant
		c.idx = -1
	} else if c.idx >= len(c.buffer.Kvs)-1 && c.buffer.More {
		c.refill()
	}
	c.idx += 1

	if c.idx >= len(c.buffer.Kvs) {
		return nil, nil, true, nil
	}
	rv := c.buffer.Kvs[c.idx]
	return c.keyToHead(rv.Key), rv.Value, false, nil
}

func (c *cursor) refill() (berr bte.BTE) {
	c.idx = 0
	rv, err := c.ec.Get(c.ctx, c.startFrom,
		etcd.WithSort(etcd.SortByKey, etcd.SortAscend),
		etcd.WithRange(etcd.GetPrefixRangeEnd(c.prefix)),
		etcd.WithLimit(cursorBufferSize),
		etcd.WithSerializable())
	if err != nil {
		return bte.ErrW(bte.EtcdFailure, "could not refill cursor", err)
	}
	c.buffer = rv
	if len(c.buffer.Kvs) > 0 {
		c.startFrom = string(c.buffer.Kvs[len(c.buffer.Kvs)-1].Key)
	}
	return nil
}

//The cursor must start before the first result, such that pop() or advanceUntilGTE will advance to the first result
func (em *etcdMetadataProvider) openCursor(ctx context.Context, category string, collection string, isCollectionPrefix bool, key string, val *string) (*cursor, bte.BTE) {
	skey := fmt.Sprintf("%s/%s/%s/%s", em.pfx, category, key, collection)
	if !isCollectionPrefix {
		skey += "/"
	}
	rv := &cursor{ec: em.ec,
		prefix:    skey,
		startFrom: skey,
		idx:       -1,
		ctx:       ctx,
		buffer:    nil}
	if val == nil {
		rv.optional = true
	} else {
		rv.desiredValue = []byte(*val)
	}
	return rv, nil
}

func headToUU(b []byte) []byte {
	if b == nil {
		return nil
	}
	return b[len(b)-16:]
}

func (em *etcdMetadataProvider) fastPathCollectionsOnly(ctx context.Context, collection string, isCollectionPrefix bool) (chan *LookupResult, chan bte.BTE) {
	lrchan := make(chan *LookupResult, 100)
	errchan := make(chan bte.BTE, 1)
	go func() {
		collection = fmt.Sprintf("%s/s/%s", em.pfx, collection)
		if !isCollectionPrefix {
			collection += "/"
		}
		fromkey := collection
		endkey := etcd.GetPrefixRangeEnd(collection)
		skip := false
		for {
			rv, err := em.ec.Get(ctx, fromkey,
				etcd.WithSort(etcd.SortByKey, etcd.SortAscend),
				etcd.WithRange(endkey),
				etcd.WithLimit(cursorBufferSize),
				etcd.WithSerializable())
			if err != nil {
				errchan <- bte.ErrW(bte.EtcdFailure, "could not refill cxcursor", err)
				return
			}
			for _, kv := range rv.Kvs {
				if skip {
					skip = false
					continue
				}
				lr, err := em.GetStreamInfo(ctx, kv.Value)
				if err != nil {
					errchan <- err
					return
				}
				lrchan <- lr
			}
			if rv.More {
				skip = true
				fromkey = string(rv.Kvs[len(rv.Kvs)-1].Key)
			} else {
				break
			}
		}
		close(lrchan)
		return
	}()
	return lrchan, errchan
}

// functions must not close error channel
// functions must not close value channel if there was an error
// functions must not write to error channel if they are blocking on sending to value channel (avoid leak)
// functions must treat a context cancel as an error and obey the above rules

func (em *etcdMetadataProvider) LookupStreams(pctx context.Context, collection string, isCollectionPrefix bool, tags map[string]*string, anns map[string]*string) (chan *LookupResult, chan bte.BTE) {
	//Check all the inputs are ok
	if err := validateCollectionTagsAndAnns(collection, tags, anns); err != nil {
		return nil, bte.Chan(err)
	}
	if len(tags) == 0 && len(anns) == 0 {
		return em.fastPathCollectionsOnly(pctx, collection, isCollectionPrefix)
	}

	lrchan := make(chan *LookupResult, 100)
	errchan := make(chan bte.BTE, 1)
	go func() {
		ctx, cancel := context.WithCancel(pctx)
		defer cancel()

		//Open all the cursors
		cz := []*cursor{}
		for k, v := range tags {
			cur, err := em.openCursor(ctx, "t", collection, isCollectionPrefix, k, v)
			if err != nil {
				errchan <- err
				return
			}
			cz = append(cz, cur)
		}
		for k, v := range anns {
			cur, err := em.openCursor(ctx, "a", collection, isCollectionPrefix, k, v)
			if err != nil {
				errchan <- err
				return
			}
			cz = append(cz, cur)
		}

		//advance the first cursor and curhead
		curhead, fin, err := cz[0].pop()
		if fin {
			close(lrchan)
			return
		}
		if err != nil {
			errchan <- err
			return
		}
	outer:
		for {
			for i := 0; i < len(cz); i++ {
				gt, thishead, fin, err := cz[i].advanceUntilGTE(curhead)
				if fin {
					close(lrchan)
					return
				}
				if err != nil {
					errchan <- err
					return
				}
				if gt {
					//This cursor is now greater. Repeat the search
					curhead = thishead
					continue outer
				}
			}
			//All the cursors are equal. This stream is a match. Emit the uuid
			lr, err := em.GetStreamInfo(ctx, headToUU(curhead))
			if err != nil {
				errchan <- err
				return
			}
			lrchan <- lr
			//Now advance the first cursor and curhead
			thishead, fin, err := cz[0].pop()
			if fin {
				close(lrchan)
				return
			}
			if err != nil {
				errchan <- err
				return
			}
			curhead = thishead
		}
	}()
	return lrchan, errchan
}
