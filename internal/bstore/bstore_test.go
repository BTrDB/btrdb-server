package bstore

import (
	"math/rand"
	"reflect"
	"testing"
	"time"
	"strings"
	_ "code.google.com/p/go-uuid/uuid"
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
		switch fld.Type().Kind() {
			case reflect.Array:
				for k := 0; k < fld.Len(); k++ {
					if fld.Type().Elem().Kind() == reflect.Float64 {
						fld.Index(k).SetFloat(mFloat64())
					} else if fld.Type().Elem().Kind() == reflect.Uint64 {
						fld.Index(k).SetUint(mUint64())
					} else if fld.Type().Elem().Kind() == reflect.Int64 {
						fld.Index(k).SetInt(mInt64())
					} else if fld.Type().Elem().Kind() == reflect.Uint8 {
						fld.Index(k).SetUint(mUint64())
					} else {
						log.Panic("Unhandled element type: %v", fld.Type().Elem().Kind())
					}
				}
			case reflect.Uint64:
				fld.SetUint(mUint64())
			case reflect.Uint8:
				fld.SetUint(mUint64()&0xFF)
			case reflect.Uint16:
				fld.SetUint(mUint64()&0xFFFF)
			case reflect.Int64:
				fld.SetInt(mInt64())
			case reflect.Int:
				fld.SetInt(mInt64())
			default:
				log.Panicf("Unrecognized type: %+v", fld.Type().Kind())
		}
	}
}
/*
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
*/
func MakeCoreblock() (*Coreblock) {
	db := new (Coreblock)
	FillBlock(db)
	for i:=0;i<KFACTOR;i++ {
		//These have special meaning, so don't test it here
		if db.Addr[i] == 0 {
			db.Addr[i] = 1
		}
	}
	return db
}

func MakeVBlock() (*Vectorblock) {
	v := new (Vectorblock)
	FillBlock(v)
	v.Len = VSIZE
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
		log.Fatalf("Types differ %v %v", vlhs.Type(), vrhs.Type())
		return false
	}
	for k := 0; k < vlhs.NumField(); k++ {
		tagstring := string(reflect.TypeOf(lhs).Field(k).Tag)
		tags := strings.Split(tagstring,",")
		doskip := false
		for _, k := range tags {
			if chk[k] {
				doskip = true
			}
		}
		if doskip {
			continue
		}
		if !reflect.DeepEqual(vlhs.Field(k).Interface(), vrhs.Field(k).Interface()) {
			log.Fatalf("Field differs: %v, %v != %v", reflect.TypeOf(lhs).Field(k).Name,
				vlhs.Field(k).Interface(), vrhs.Field(k).Interface())
			return false
		}
	}
	return true
}

/*
var _bs *BlockStore = nil
var _gen *Generation = nil
func mBS() {
	testuuid := uuid.NewRandom()
	nbs, err := NewBlockStore("localhost", 0, "/srv/quasartestdb/")
	if err != nil {
		log.Panic(err)
	}
	if _bs == nil {
		_bs = nbs
		_gen = _bs.ObtainGeneration(testuuid)
	}
}
*/


func TestCoreBlockSERDES(t *testing.T) {
	db := MakeCoreblock()
	buf := make([]byte, CBSIZE)
	db.Serialize(buf)
	out := new(Coreblock)
	out.Deserialize(buf)
	if !CompareNoTags(*db, *out, []string{"implicit"}) {
		t.Error("Core block SERDES faled")
	}
}

func TestCoreBlockSERDESAbsFullZero(t *testing.T) {
	db := MakeCoreblock()
	db.Addr[10] = 0
	db.Min[10] = 0
	db.Mean[10] = 0
	db.Max[10] = 0
	db.Count[10] = 0
	
	db.Addr[11] = 0
	db.Min[11] = 0
	db.Mean[11] = 0
	db.Max[11] = 0
	db.Count[11] = 0
	db.CGeneration[11] = 0
	
	db.Addr[54] = 0
	db.Min[54] = 0
	db.Mean[54] = 0
	db.Max[54] = 0
	db.Count[54] = 0
	
	for i:=55;i<KFACTOR;i++ {
		db.Addr[i] = 0
		db.Min[i] = 0
		db.Mean[i] = 0
		db.Max[i] = 0
		db.Count[i] = 0
		db.CGeneration[i] = 0
	}
	
	buf := make([]byte, CBSIZE)
	db.Serialize(buf)
	out := new(Coreblock)
	out.Deserialize(buf)
	
	if !CompareNoTags(*db, *out, []string{"implicit"}) {
		t.Error("Core block SERDES faled")
	}
}

func TestCoreBlockBadDES(t *testing.T) {
	db := MakeCoreblock()
	buf := make([]byte, CBSIZE)
	db.Serialize(buf)
	out := new(Coreblock)
	out.Deserialize(buf)
	if (out.GetDatablockType() != Core) {
		t.FailNow()
	}
	defer func() {
		if r := recover(); r == nil {
			//We expected a failure
			t.FailNow()
		}
	}()
	vb := new(Vectorblock)
	vb.Deserialize(buf)
	t.FailNow()
}
func TestVectorBlockBadDES(t *testing.T) {
	v := MakeVBlock()
	buf := make([]byte, VBSIZE)
	v.Serialize(buf)
	out := new(Vectorblock)
	out.Deserialize(buf)
	if (out.GetDatablockType() != Vector) {
		t.Fatal("Wrong id on block")
	}
	defer func() {
		if r := recover(); r == nil {
			//We expected a failure
			t.Fatal("Did not throw exception")
		}
	}()
	cb := new(Coreblock)
	cb.Deserialize(buf)
	t.FailNow()
}
func TestBufferType(t *testing.T) {
	v := MakeVBlock()
	buf := make([]byte, VBSIZE)
	v.Serialize(buf)
	if DatablockGetBufferType(buf) != Vector {
		t.Fatal("Expected Vector")
	}
	c := MakeCoreblock()
	buf2 := make([]byte, CBSIZE)
	c.Serialize(buf2)
	if DatablockGetBufferType(buf2) != Core {
		t.Fatal("Expected Core")
	}
	buf3 := make([]byte, 2)
	buf3[0] = byte(5)
	if DatablockGetBufferType(buf3) != Bad {
		t.Fatal("Expected Bad")
	}
}
func TestVBlockSERDES(t *testing.T) {
	v := MakeVBlock()
	buf := make([]byte, VBSIZE)
	v.Serialize(buf)
	out := new(Vectorblock)
	out.Deserialize(buf)
	if !CompareNoTags(*v, *out, []string{"implicit"}) {
		t.Error("Vector block SERDES failed")
	}
}


func TestVBlockManSERDES(t *testing.T) {
	v := new(Vectorblock)
	for i :=0; i < 6; i++ {
		v.Time[i] = int64(i*100000)
		v.Value[i] = float64(i*100000.0)
	}
	v.Len = 6
	buf := make([]byte, VBSIZE)
	v.Serialize(buf)
	out := new(Vectorblock)
	out.Deserialize(buf)
	for i :=0; i < 6; i++ {
		if v.Value[i] != out.Value[i] {
			t.Error("Fail")
		}
	}
}

/*
func TestCBlockE2ESERDES(t *testing.T) {
	db:= MakeCoreblock()
	cpy := *db
	if err := _gen.Commit(); err != nil {
		t.Error(err)
	}
	_bs = nil
	_gen = nil
	mBS()
	out := _bs.ReadDatablock(cpy.This_addr)
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
	out := _bs.ReadDatablock(cpy.This_addr)
	if !CompareNoTags(cpy,*(out.(*Vectorblock)),[]string{"volatile"}) {
		t.Error("E2E V SERDES failed")
	}
}
*/
func TestVCopyInto(t *testing.T) {
	db:= MakeVBlock()
	out := &Vectorblock{}
	db.CopyInto(out)
	if !CompareNoTags(*db,*out, []string{"metadata"}) {
		t.Error("V CopyInto failed")
	}
}


func TestCCopyInto(t *testing.T) {
	db:= MakeCoreblock()
	out := &Coreblock{}
	db.CopyInto(out)
	if !CompareNoTags(*db,*out, []string{"metadata"}) {
		t.Error("C CopyInto failed")
	}
}

/*
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
*/








