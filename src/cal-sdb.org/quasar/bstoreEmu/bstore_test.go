package bstoreEmu

import (
	"log"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

func mUint64() uint64 {
	return uint64(rand.Uint32())
	//return (uint64(rand.Uint32()) << 32) + uint64(rand.Uint32())
}
func mInt64() int64 {
	return int64(mUint64())
}
func mFloat64() float64 {
	return rand.Float64()
}

var testuuid UUID = UUID([...]byte{0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0})

func mUUID() UUID {
	g := [16]byte{}
	for i:=0; i < 16; i++ {
		g[i] = uint8(rand.Int())
	}
	return UUID(g)
}
/**
 * Randomly populate the fields of a struct
 */
func FillBlock(rv interface {}) {
	rand.Seed(time.Now().UnixNano())
	t := reflect.ValueOf(rv)
	for i := 0; i < t.Elem().NumField(); i++ {
		fld := t.Elem().Field(i)
		switch fld.Type().Kind() {
			case reflect.Array:
				for k := 0; k < fld.Len(); k++ {
					if fld.Type().Elem().Kind() == reflect.Float64 {
						fld.Index(k).SetFloat(mFloat64())
					} else if fld.Type().Elem().Kind() == reflect.Uint64 {
						fld.Index(k).SetUint(mUint64())
					} else if fld.Type().Elem().Kind() == reflect.Int64 {
						fld.Index(k).SetInt(mInt64())
					} else {
						log.Panic("Unhandled element type")
					}
				}
			case reflect.Uint64:
				fld.SetUint(mUint64())
			case reflect.Uint8:
				fld.SetUint(mUint64()&0xFF)
			case reflect.Int64:
				fld.SetInt(mInt64())
			case reflect.Int:
				fld.SetInt(mInt64())
			default:
				log.Panicf("Unrecognized type: %+v", fld.Type().Kind())
		}
	}
}
func MakeCoreblock() (*Coreblock) {
	mBS()
	db, err := _gen.AllocateCoreblock()
	if err != nil {
		log.Panic(err)
	} 
	addr := db.This_addr
	FillBlock(db)
	db.This_addr = addr
	return db
}

func MakeVBlock() (*Vectorblock) {
	mBS()
	v, err := _gen.AllocateVectorblock()
	if err != nil {
		log.Panic(err)
	}
	addr := v.This_addr
	FillBlock(v)
	v.This_addr = addr
	return v
}
/**
 * This should work with any object that uses the struct tags to
 * mean fields that don't need to match after SERDES
 */
func CompareNoTags(lhs interface{}, rhs interface{}, tags []string) bool {
	chk := make(map[string]bool)
	for _, s := range tags {
		chk[s] = true
	}
	vlhs := reflect.ValueOf(lhs)
	vrhs := reflect.ValueOf(rhs)
	if vlhs.Type() != vrhs.Type() {
		log.Printf("Types differ %v %v", vlhs.Type(), vrhs.Type())
		return false
	}
	for k := 0; k < vlhs.NumField(); k++ {
		_, skip := chk[string(reflect.TypeOf(lhs).Field(k).Tag)]
		if skip {
			continue
		}
		if !reflect.DeepEqual(vlhs.Field(k).Interface(), vrhs.Field(k).Interface()) {
			log.Printf("Field differs: %v, %v != %v", reflect.TypeOf(lhs).Field(k).Name,
				vlhs.Field(k).Interface(), vrhs.Field(k).Interface())
			return false
		}
	}
	return true
}

var _bs *BlockStore = nil
var _gen *Generation = nil
func mBS() {
	nbs, err := NewBlockStore("localhost", 0)
	if err != nil {
		log.Panic(err)
	}
	if _bs == nil {
		_bs = nbs
		_gen = _bs.ObtainGeneration(testuuid)
	}
}
func TestCoreBlockSERDES(t *testing.T) {
	db := MakeCoreblock()
	buf := make([]byte, DBSIZE)
	db.Serialize(buf)
	out := new(Coreblock)
	out.Deserialize(buf)
	if !CompareNoTags(*db, *out, []string{"volatile"}) {
		t.Error("Data block SERDES faled")
	}
}

func TestVBlockSERDES(t *testing.T) {
	v := MakeVBlock()
	buf := make([]byte, DBSIZE)
	v.Serialize(buf)
	out := new(Vectorblock)
	out.Deserialize(buf)
	if !CompareNoTags(*v, *out, []string{"volatile"}) {
		t.Error("Vector block SERDES failed")
	}
}

func TestCBlockE2ESERDES(t *testing.T) {
	db:= MakeCoreblock()
	cpy := *db
	if err := _gen.Commit(); err != nil {
		t.Error(err)
	}
	_bs = nil
	_gen = nil
	mBS()
	out, err := _bs.ReadDatablock(cpy.This_addr)
	if err != nil {
		log.Panic(err)
	}
	if !CompareNoTags(cpy,*(out.(*Coreblock)), []string{"volatile"}) {
		t.Error("E2E C SERDES failed")
	}
}
func TestVBlockE2ESERDES(t *testing.T) {
	db:= MakeVBlock()
	cpy := *db
	if err := _gen.Commit(); err != nil {
		t.Error(err)
	}
	_bs = nil
	_gen = nil
	mBS()
	out, err := _bs.ReadDatablock(cpy.This_addr)
	if err != nil {
		log.Panic(err)
	}
	if !CompareNoTags(cpy,*(out.(*Vectorblock)),[]string{"volatile"}) {
		t.Error("E2E V SERDES failed")
	}
}

func TestVCopyInto(t *testing.T) {
	db:= MakeVBlock()
	out := &Vectorblock{}
	db.CopyInto(out)
	if !CompareNoTags(*db,*out, []string{"volatile", "metadata"}) {
		t.Error("V CopyInto failed")
	}
}

func TestCCopyInto(t *testing.T) {
	db:= MakeCoreblock()
	out := &Coreblock{}
	db.CopyInto(out)
	if !CompareNoTags(*db,*out, []string{"volatile", "metadata"}) {
		t.Error("C CopyInto failed")
	}
}

func BenchmarkSERDER(b *testing.B) {
	dblocks_in := make([]*Coreblock, b.N)
	for i := 0; i < b.N; i++ {
		dblocks_in[i] = MakeCoreblock()
	}
	dblocks_out := make([]*Coreblock, b.N)
	for i := 0; i < b.N; i++ {
		dblocks_out[i] = new(Coreblock)
	}
	buf := make([]byte, DBSIZE)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dblocks_in[0].Serialize(buf)
		dblocks_out[0].Deserialize(buf)
	}
}

/*
func TestSuperblockCommit(t *testing.T) {
	mBS()
	gen := _bs.ObtainGeneration(mUUID())
}
*/









