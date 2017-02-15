package mprovider

import (
	"context"
	"regexp"

	"github.com/SoftwareDefinedBuildings/btrdb/bte"
)

const MaximumTags = 64
const MaximumAnnotations = 64
const MaxKeyLength = 64
const MaxValLength = 256

//The regex for collections, tag keys or annotation keys
var ValidIdent = regexp.MustCompile("^[a-z0-9_-]")

type LookupResult struct {
	UUID              []byte
	Collection        string
	Tags              map[string]string
	Annotations       map[string]string
	AnnotationVersion uint64
}

type MProvider interface {

	// Sets the stream annotations. An entry with a nil string implies delete
	SetStreamAnnotations(ctx context.Context, uuid []byte, aver uint64, changes map[string]*string) bte.BTE

	// Get a stream annotations and tags
	GetStreamInfo(ctx context.Context, uuid []byte) (res *LookupResult, err bte.BTE)

	// CreateStream makes a stream with the given uuid, collection and tags. Returns
	// an error if the uuid already exists.
	CreateStream(ctx context.Context, uuid []byte, collection string, tags map[string]string, annotations map[string]string) bte.BTE

	// DeleteStream tombstones a stream
	DeleteStream(ctx context.Context, uuid []byte) bte.BTE

	// ListCollections returns a list of collections beginning with prefix (which may be "")
	// and starting from the given string. If number is > 0, only that many results
	// will be returned. More can be obtained by re-calling ListCollections with
	// a given startingFrom and number.
	ListCollections(ctx context.Context, prefix string, startingFrom string, number int64) ([]string, bte.BTE)

	// Return back all streams in all collections beginning with collection (or exactly equal if prefix is false)
	// provided they have the given tags and annotations, where a nil entry in the map means has the tag but the value is irrelevant
	LookupStreams(ctx context.Context, collection string, isCollectionPrefix bool, tags map[string]*string, annotations map[string]*string) (chan *LookupResult, chan bte.BTE)
}

type etcdMetadataProvider struct {
}

func (em *etcdMetadataProvider) doWeHoldWriteLock(uuid []byte) bool {
	return true
}
func (em *etcdMetadataProvider) SetStreamAnnotations(ctx context.Context, uuid []byte, aver uint64, changes map[string]*string) bte.BTE {
	/*
	  read /uuids/uuid full record
	  if ver != aver
	    error
	  modify it
	  transact on aver
	    set /uuids/uuid
	    set or delete each anns/<name>/<uuid>
	*/
	/*
	  caching:
	  not really required, set is rare
	*/
}
func (em *etcdMetadataProvider) GetStreamInfo(ctx context.Context, uuid []byte) (res *LookupResult, err bte.BTE) {
	/*
	  read /uuids/uuid full record
	  return
	*/
	/*
	  caching:
	  we can cache uuid->full record if we watch and invalidate /uuids/
	*/
}
func (em *etcdMetadataProvider) CreateStream(ctx context.Context, uuid []byte, collection string, tags map[string]string, annotations map[string]string) bte.BTE {
	/*
	  transact
	  check tombstone/uuid -> error if exists
	  create /uuids/uuid full record if not exists
	  create /streams/collection/uuid
	  create each tags/name/uuid
	  create each anns/name/uuid
	  create collections/<collection> if not exists
	*/
}
func (em *etcdMetadataProvider) DeleteStream(ctx context.Context, uuid []byte) bte.BTE {
	/*
	  read full record
	  txn if uuids/uuid same version
	    delete uuids/uuid
	    delete streams/<collection>/<uuid>
	    create tombstone/uuid
	    create todelete/uuid
	  if there are no streams/<collection>/*
	    delete collections/<collection>

	  outside txn? would race with queries. Prefer inside txn, benchmark.
	  delete all tags/<uuid>
	  delete all anns/<uuid>
	*/
}
func (em *etcdMetadataProvider) ListCollections(ctx context.Context, prefix string, startingFrom string, number int64) ([]string, bte.BTE) {
	/*
	  get streams/collection with prefix and count
	*/
}
func (em *etcdMetadataProvider) LookupStreams(ctx context.Context, collection string, isCollectionPrefix bool, tags map[string]*string, annotations map[string]*string) (chan *LookupResult, chan bte.BTE) {
	/*
	  let P be permitted parallelism.
	  execute P tags/<name>/<coll prefix> queries, sorted lexically
	  merge results into a list of uuids
	  while tags and annotations are empty
	  merge results
	*/
}

/*
todelete/<uuid>
tombstone/<uuid>
uuids/<uuid> -> full record (canonical aver)
collections/<collection>
streams/<collection>/<uuid> -> tags
tags/<name>/<collection>/<uuid> -> tag value
anns/<name>/<collection/<uuid> -> ann value
*/
