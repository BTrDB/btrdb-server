package bstore

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
/**
 * Randomly populate the fields of a struct
 */
func FillBlock(rv interface {}) {
	rand.Seed(time.Now().UnixNano())
	t := reflect.ValueOf(rv)
	for i := 0; i < t.Elem().NumField(); i++ {
		fld := t.Elem().Field(i)
		if fld.Type().Kind() == reflect.Array {
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
		} else if fld.Type().Kind() == reflect.Uint64 {
			fld.SetUint(mUint64())
		} else {
			log.Panic("Unrecognized type")
		}
	}
}
func MakeDBlock2(bs *BlockStore) (*Coreblock) {
	db, err := bs.AllocateDataBlock()
	if err != nil {
		log.Panic(err)
	}
	addr := db.This_addr
	FillBlock(db)
	db.This_addr = addr
	return db
}

func MakeVBlock(bs *BlockStore) (*Vectorblock) {
	v, err := bs.AllocateVectorBlock()
	if err != nil {
		log.Panic(err)
	}
	addr := v.This_addr
	FillBlock(v)
	v.This_addr = addr
	return v
}
/**
 * This should work with any object that uses the "volatile" struct tag to
 * mean fields that don't need to match after SERDES
 */
func CompareNoVolatile(lhs interface{}, rhs interface{}) bool {
	vlhs := reflect.ValueOf(lhs)
	vrhs := reflect.ValueOf(rhs)
	if vlhs.Type() != vrhs.Type() {
		log.Printf("Types differ %v %v", vlhs.Type(), vrhs.Type())
		return false
	}
	for k := 0; k < vlhs.NumField(); k++ {
		if string(reflect.TypeOf(lhs).Field(k).Tag) == "volatile" {
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

func mBS() (* BlockStore) {
	bs := BlockStore{}
	bs.Init("localhost")
	return &bs
}
func TestBlockSERDES(t *testing.T) {
	bs := mBS()

	db := MakeDBlock2(bs)
	buf := make([]byte, DBSIZE)
	db.Serialize(buf)
	out := new(Coreblock)
	out.Deserialize(buf)
	if !CompareNoVolatile(*db, *out) {
		t.Error("Data block SERDES faled")
	}
}

func TestVBlockSERDES(t *testing.T) {
	bs := mBS()
	v := MakeVBlock(bs)
	buf := make([]byte, DBSIZE)
	v.Serialize(buf)
	out := new(Vectorblock)
	out.Deserialize(buf)
	if !CompareNoVolatile(*v, *out) {
		t.Error("Vector block SERDES failed")
	}
}

func TestBlockE2ESERDES(t *testing.T) {
	bs := mBS()
	db:= MakeDBlock2(bs)
	cpy := *db
	bs.WriteCoreblockAndFree(db)
	out, err := bs.ReadCoreblock(cpy.This_addr)
	if err != nil {
		log.Panic(err)
	}
	if !CompareNoVolatile(cpy, *out) {
		t.Error("E2E SERDES failed")
	}
}

func BenchmarkSERDER(b *testing.B) {
	bs := new(BlockStore)
	bs.Init("localhost")
	dblocks_in := make([]*Coreblock, b.N)
	for i := 0; i < b.N; i++ {
		dblocks_in[i] = MakeDBlock2(bs)
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









