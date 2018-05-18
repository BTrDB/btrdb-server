package mprovider

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/BTrDB/btrdb-server/bte"
	etcd "github.com/coreos/etcd/clientv3"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pborman/uuid"
)

const MaximumTags = 32
const MaximumAnnotations = 64
const MaxTagKeyLength = 64
const MaxTagValLength = 256
const MaxAnnKeyLength = 64
const MaxAnnValLength = 256
const MaxListCollections = 10000
const MaxCollectionLength = 256

var tagKeysRegex = regexp.MustCompile(`^[a-z][a-z0-9_.]*$`)
var annKeysRegex = tagKeysRegex

func isValidTagKey(k string) bool {
	return len(k) < MaxTagKeyLength && len(k) > 0 && tagKeysRegex.MatchString(k)
}
func isValidAnnKey(k string) bool {
	return len(k) < MaxAnnKeyLength && len(k) > 0 && annKeysRegex.MatchString(k)
}
func isValidTagValue(k string) bool {
	return len(k) < MaxTagValLength && len(k) > 0 && !bytes.Contains([]byte(k), []byte{0})
}
func isValidAnnotationValue(k string) bool {
	return len(k) < MaxAnnValLength
}
func isValidCollection(k string) bool {
	return (len(k) < MaxCollectionLength &&
		len(k) > 0 &&
		!bytes.Contains([]byte(k), []byte{0}) &&
		utf8.Valid([]byte(k)))
}

type LookupResult struct {
	UUID              []byte
	Collection        string
	Tags              map[string]string
	Annotations       map[string]string
	AnnotationVersion uint64
}

func (lr *LookupResult) String() string {
	if lr == nil {
		return "(nil LookupResult)"
	}
	tagstrz := []string{}
	for k, v := range lr.Tags {
		tagstrz = append(tagstrz, fmt.Sprintf("%s=%s", k, v))
	}
	sort.StringSlice(tagstrz).Sort()

	annstrz := []string{}
	for k, v := range lr.Annotations {
		annstrz = append(annstrz, fmt.Sprintf("%s=%s", k, v))
	}
	sort.StringSlice(annstrz).Sort()
	uuidst := uuid.UUID(lr.UUID).String()

	return fmt.Sprintf("([%s] %s tags=(%q) anns=(%q) aver=%d)", uuidst, lr.Collection, strings.Join(tagstrz, ","), strings.Join(annstrz, ","), lr.AnnotationVersion)
}
func uuidToString(uu []byte) string {
	return uuid.UUID(uu).String()
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
	ListCollections(ctx context.Context, prefix string, startingFrom string, limit uint64) ([]string, bte.BTE)

	// Return back all streams in all collections beginning with collection (or exactly equal if prefix is false)
	// provided they have the given tags and annotations, where a nil entry in the map means has the tag but the value is irrelevant
	LookupStreams(ctx context.Context, collection string, isCollectionPrefix bool, tags map[string]*string, annotations map[string]*string) (chan *LookupResult, chan bte.BTE)

	// Return back a list of uuids that need to be deleted in the background
	ListToDelete(ctx context.Context) ([][]byte, bte.BTE)

	// Remove the given list of uuids from the background deletion queue
	ClearToDelete(ctx context.Context, uuids [][]byte) bte.BTE

	// Get which tags and annotations are in use in the given collection prefix
	GetKeyUsage(ctx context.Context, collectionPrefix string) (map[string]int, map[string]int, bte.BTE)
}

type etcdMetadataProvider struct {
	ec  *etcd.Client
	pfx string
}

func NewEtcdMetadataProvider(pfx string, client *etcd.Client) MProvider {
	return &etcdMetadataProvider{pfx: pfx, ec: client}
}
func (em *etcdMetadataProvider) doWeHoldWriteLock(uuid []byte) bool {
	return true
}

func (em *etcdMetadataProvider) SetStreamAnnotations(ctx context.Context, uuid []byte, aver uint64, changes map[string]*string) bte.BTE {
	span, ctx := opentracing.StartSpanFromContext(ctx, "SetStreamAnnotations")
	defer span.Finish()
	for k, v := range changes {
		if !isValidAnnKey(k) {
			return bte.Err(bte.InvalidTagKey, fmt.Sprintf("annotation key %q is invalid", k))
		}
		if v != nil {
			if !isValidAnnotationValue(*v) {
				return bte.Err(bte.InvalidTagValue, fmt.Sprintf("annotation value for key %q is invalid", k))
			}
		}
	}
	streamkey := fmt.Sprintf("%s/u/%s", em.pfx, string(uuid))
	rv, err := em.ec.Get(ctx, streamkey)
	if err != nil {
		return bte.ErrW(bte.EtcdFailure, "could not obtain stream record", err)
	}
	if rv.Count == 0 {
		return bte.Err(bte.NoSuchStream, "stream does not exist")
	}
	fullrec := rv.Kvs[0]
	if fullrec.Version != int64(aver) {
		fmt.Printf("[TRACE] stream ann ver expected %d got %d\n", int64(aver), fullrec.Version)
		return bte.Err(bte.AnnotationVersionMismatch, "stream annotation version does not match")
	}
	fr := em.decodeFullRecord(fullrec.Value)
	opz := []etcd.Op{}
	for k, v := range changes {
		keypath := fmt.Sprintf("%s/a/%s/%s/%s", em.pfx, k, fr.Collection, string(uuid))
		if v == nil {
			fr.deleteAnnotation(k)
			opz = append(opz, etcd.OpDelete(keypath))
		} else {
			fr.setAnnotation(k, *v)
			opz = append(opz, etcd.OpPut(keypath, *v))
		}
	}

	frbin := fr.Serialize()
	opz = append(opz, etcd.OpPut(streamkey, string(frbin)))
	txres, err := em.ec.Txn(ctx).
		If(etcd.Compare(etcd.Version(streamkey), "=", int64(aver))).
		Then(opz...).
		Commit()
	if err != nil {
		return bte.ErrW(bte.EtcdFailure, "could not update annotation", err)
	}
	if !txres.Succeeded {
		return bte.Err(bte.AnnotationVersionMismatch, "stream annotation version does not match")
	}
	return nil

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
func (em *etcdMetadataProvider) GetStreamInfo(ctx context.Context, uuid []byte) (*LookupResult, bte.BTE) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "GetStreamInfo")
	defer span.Finish()
	streamkey := fmt.Sprintf("%s/u/%s", em.pfx, string(uuid))
	rv, err := em.ec.Get(ctx, streamkey, etcd.WithSerializable())
	if err != nil {
		return nil, bte.ErrW(bte.EtcdFailure, "could not obtain stream record", err)
	}
	if rv.Count == 0 {
		return nil, bte.Err(bte.NoSuchStream, "stream does not exist")
	}
	fullrec := rv.Kvs[0]
	fr := em.decodeFullRecord(fullrec.Value)
	return &LookupResult{
		UUID:              uuid,
		Collection:        fr.Collection,
		Tags:              fr.Tags,
		Annotations:       fr.Anns,
		AnnotationVersion: uint64(fullrec.Version),
	}, nil

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
	span, ctx := opentracing.StartSpanFromContext(ctx, "CreateStream")
	defer span.Finish()
	if !isValidCollection(collection) {
		return bte.Err(bte.InvalidCollection, fmt.Sprintf("collection %q is invalid", collection))
	}
	for k, v := range tags {
		if !isValidTagKey(k) {
			return bte.Err(bte.InvalidTagKey, fmt.Sprintf("tag key %q is invalid", k))
		}
		if !isValidTagValue(v) {
			return bte.Err(bte.InvalidTagValue, fmt.Sprintf("tag value for key %q is invalid", k))
		}
	}
	for k, v := range annotations {
		if !isValidAnnKey(k) {
			return bte.Err(bte.InvalidTagKey, fmt.Sprintf("annotation key %q is invalid", k))
		}
		if !isValidAnnotationValue(v) {
			return bte.Err(bte.InvalidTagValue, fmt.Sprintf("annotation value for key %q is invalid", k))
		}
	}
	if tags == nil {
		tags = make(map[string]string)
	}
	if annotations == nil {
		annotations = make(map[string]string)
	}
	fr := &FullRecord{
		Tags:       tags,
		Anns:       annotations,
		Collection: collection,
	}
	streamkey := fmt.Sprintf("%s/u/%s", em.pfx, string(uuid))
	tombstonekey := fmt.Sprintf("%s/z/%s", em.pfx, string(uuid))
	opz := []etcd.Op{}
	opz = append(opz, etcd.OpPut(streamkey, string(fr.Serialize())))
	for k, v := range tags {
		path := fmt.Sprintf("%s/t/%s/%s/%s", em.pfx, k, collection, string(uuid))
		opz = append(opz, etcd.OpPut(path, v))
	}
	for k, v := range annotations {
		path := fmt.Sprintf("%s/a/%s/%s/%s", em.pfx, k, collection, string(uuid))
		opz = append(opz, etcd.OpPut(path, v))
	}
	//Although this may exist, it is important to write to it again
	//because the delete code will transact on the version of this
	colpath := fmt.Sprintf("%s/c/%s/", em.pfx, collection)
	opz = append(opz, etcd.OpPut(colpath, "NA"))
	tagstring := tagString(tags)
	tagstringpath := fmt.Sprintf("%s/s/%s/%s", em.pfx, collection, tagstring)
	opz = append(opz, etcd.OpPut(tagstringpath, string(uuid)))
	txr, err := em.ec.Txn(ctx).
		If(etcd.Compare(etcd.Version(tombstonekey), "=", 0),
			etcd.Compare(etcd.Version(streamkey), "=", 0),
			etcd.Compare(etcd.Version(tagstringpath), "=", 0)).
		Then(opz...).
		Commit()
	if err != nil {
		return bte.ErrW(bte.EtcdFailure, "could not create stream", err)
	}
	if !txr.Succeeded {
		//Perhaps tagstring collided
		kv, err := em.ec.Get(ctx, tagstringpath)
		if err != nil {
			return bte.ErrW(bte.EtcdFailure, "could not create stream", err)
		}
		if kv.Count != 0 {
			return bte.Err(bte.StreamExists, fmt.Sprintf("a stream already exists in that collection with identical tags"))
		}

		//Perhaps stream uuid exists, otherwise it was tombstone
		kv, err = em.ec.Get(ctx, streamkey)
		if err != nil {
			return bte.ErrW(bte.EtcdFailure, "could not create stream", err)
		}
		if kv.Count == 0 {
			return bte.Err(bte.ReusedUUID, fmt.Sprintf("uuid %q has been used before with a (now deleted) stream", uuidToString(uuid)))
		} else {
			return bte.Err(bte.ReusedUUID, fmt.Sprintf("a stream already exists with uuid %q and a different collection or tags", uuidToString(uuid)))
		}
	}
	return nil
	/*
	  transact
	  check tombstone/uuid -> error if exists
	  create /uuids/uuid full record if not exists
	  create /streams/collection/canonicaltagstring
	  create each tags/name/uuid
	  create each anns/name/uuid
	  create collections/<collection>
	*/
}

func (em *etcdMetadataProvider) DeleteStream(ctx context.Context, uuid []byte) bte.BTE {
	span, ctx := opentracing.StartSpanFromContext(ctx, "DeleteStream")
	defer span.Finish()
	streamkey := fmt.Sprintf("%s/u/%s", em.pfx, string(uuid))
	rv, err := em.ec.Get(ctx, streamkey)
	if err != nil {
		return bte.ErrW(bte.EtcdFailure, "could not obtain stream record", err)
	}
	if rv.Count == 0 {
		return bte.Err(bte.NoSuchStream, "stream does not exist")
	}
	fullrec := rv.Kvs[0]
	fr := em.decodeFullRecord(fullrec.Value)

	tombstonekey := fmt.Sprintf("%s/z/%s", em.pfx, string(uuid))
	todeletekey := fmt.Sprintf("%s/d/%s", em.pfx, string(uuid))

	opz := []etcd.Op{}
	opz = append(opz, etcd.OpPut(todeletekey, "NA"))
	opz = append(opz, etcd.OpDelete(streamkey))
	opz = append(opz, etcd.OpPut(tombstonekey, "NA"))

	for k, _ := range fr.Tags {
		path := fmt.Sprintf("%s/t/%s/%s/%s", em.pfx, k, fr.Collection, string(uuid))
		opz = append(opz, etcd.OpDelete(path))
	}
	for k, _ := range fr.Anns {
		path := fmt.Sprintf("%s/a/%s/%s/%s", em.pfx, k, fr.Collection, string(uuid))
		opz = append(opz, etcd.OpDelete(path))
	}

	tagstring := tagString(fr.Tags)
	tagstringpath := fmt.Sprintf("%s/s/%s/%s", em.pfx, fr.Collection, tagstring)
	opz = append(opz, etcd.OpDelete(tagstringpath))

	txr, err := em.ec.Txn(ctx).
		If(etcd.Compare(etcd.Version(streamkey), "=", fullrec.Version)).
		Then(opz...).
		Commit()
	if err != nil {
		return bte.ErrW(bte.EtcdFailure, "could not delete stream", err)
	}
	if !txr.Succeeded {
		return bte.Err(bte.ConcurrentModification, "delete aborted: stream attributes changed")
	}

	//Now we also need to potentiall delete the collection record
	colpath := fmt.Sprintf("%s/c/%s/", em.pfx, fr.Collection)
	ckv, err := em.ec.Get(ctx, colpath)
	if err != nil {
		return bte.ErrW(bte.EtcdFailure, "could not delete stream", err)
	}
	if ckv.Count == 0 {
		//no need to delete col
		return nil
	}
	ver := ckv.Kvs[0].Version

	crprefix := fmt.Sprintf("%s/s/%s/", em.pfx, fr.Collection)
	kv, err := em.ec.Get(ctx, crprefix, etcd.WithPrefix())
	if err != nil {
		return bte.ErrW(bte.EtcdFailure, "could not delete stream", err)
	}
	if kv.Count == 0 {
		//We need to delete the collection head
		_, err := em.ec.Txn(ctx).
			If(etcd.Compare(etcd.Version(colpath), "=", ver)).
			Then(etcd.OpDelete(colpath)).
			Commit()
		if err != nil {
			return bte.ErrW(bte.EtcdFailure, "could not delete stream", err)
		}
	}
	return nil
	/*
	  read full record
	  txn if uuids/uuid same version
	    delete uuids/uuid
	    delete streams/<collection>/<tagstring>
	    create tombstone/uuid
	    create todelete/uuid
	  if there are no streams/<collection>/*
	    delete collections/<collection>

	  outside txn? would race with queries. Prefer inside txn, benchmark.
	  delete all tags/<uuid>
	  delete all anns/<uuid>
	*/
}

func (em *etcdMetadataProvider) ListCollections(ctx context.Context, prefix string, startingFrom string, limit uint64) ([]string, bte.BTE) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "ListCollections")
	defer span.Finish()
	/*
	  get streams/collection with prefix and count
	*/
	if !strings.HasPrefix(startingFrom, prefix) {
		return nil, bte.Err(bte.WrongArgs, "starting parameter does not have the prefix")
	}
	if limit == 0 || limit > MaxListCollections {
		return nil, bte.Err(bte.WrongArgs, fmt.Sprintf("limit parameter must be 0 < limit <= %d", MaxListCollections))
	}
	if prefix != "" && !isValidCollection(prefix) {
		return nil, bte.Err(bte.InvalidCollection, fmt.Sprintf("prefix %q is not a valid collection prefix", prefix))
	}
	ourprefix := fmt.Sprintf("%s/c/", em.pfx)
	path := fmt.Sprintf("%s/c/%s", em.pfx, startingFrom)
	fullprefix := fmt.Sprintf("%s/c/%s", em.pfx, prefix)
	kv, err := em.ec.Get(ctx, path, etcd.WithRange(etcd.GetPrefixRangeEnd(fullprefix)), etcd.WithLimit(int64(limit)), etcd.WithSerializable())
	if err != nil {
		return nil, bte.ErrW(bte.EtcdFailure, "could not enumerate collections", err)
	}
	rv := make([]string, 0, kv.Count)
	for _, elem := range kv.Kvs {
		p := strings.TrimSuffix(strings.TrimPrefix(string(elem.Key), ourprefix), "/")
		rv = append(rv, p)
	}
	return rv, nil
}

func (em *etcdMetadataProvider) ListToDelete(ctx context.Context) ([][]byte, bte.BTE) {
	path := fmt.Sprintf("%s/d/", em.pfx)
	kv, err := em.ec.Get(ctx, path, etcd.WithPrefix(), etcd.WithSerializable())
	if err != nil {
		return nil, bte.ErrW(bte.EtcdFailure, "could not enumerate delete tasks", err)
	}
	rv := [][]byte{}
	for _, elem := range kv.Kvs {
		p := elem.Key[len(elem.Key)-16:]
		rv = append(rv, p)
	}
	return rv, nil
}

func (em *etcdMetadataProvider) ClearToDelete(ctx context.Context, uuids [][]byte) bte.BTE {
	for _, u := range uuids {
		path := fmt.Sprintf("%s/d/%s", em.pfx, string(u))
		_, err := em.ec.Delete(ctx, path)
		if err != nil {
			return bte.ErrW(bte.EtcdFailure, "could not delete pending task", err)
		}
	}
	return nil
}

/*
d/<uuid> -> "NA"            #todelete
x/<uuid> -> "NA"            #tombstone
u/<uuid> -> fullrecord      #uuids/<uuid>
c/<collection>/ -> "NA"      #collections/<collection> #note trailing slash
s/<collection>/<tagstring>  -> uuid #get streams inside collection and verify non duplicate tags
t/<tagname>/<collection>/<uuid> -> <tag value>
a/<annname>/<collection>/<uuid> -> <ann value>
*/

/*
todelete/<uuid>
tombstone/<uuid>
uuids/<uuid> -> full record (canonical aver)
collections/<collection>
streams/<collection>/<uuid> -> tags
tags/<name>/<collection>/<uuid> -> tag value
anns/<name>/<collection/<uuid> -> ann value
*/
