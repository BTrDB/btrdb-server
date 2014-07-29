package bstore

import (
	"encoding/binary"
	"math"
)

//We are aiming for datablocks to be 8Kbytes
const KFACTOR = 64
const VSIZE = 256
const DBSIZE = 8192

type Superblock struct {
	uuid UUID
	gen  uint64
	root uint64
}

func (s *Superblock) Create(uuid UUID) {
	//TODO
}

func (s *Superblock) Clone() *Superblock {
	//TODO
	return nil
}

type BlockType uint64
const (
	Vector BlockType = 1
	Core BlockType = 2
	Bad BlockType = 255
)
type Datablock interface {
	GetDatablockType() BlockType
}

type Vectorblock struct {
	Type		uint64
	This_addr  uint64
	Generation uint64
	
	Time       [VSIZE]int64
	Value	   [VSIZE]float64
}
func (*Vectorblock) GetDatablockType() BlockType {
	return Vector
}

type Coreblock struct {
	Type	   uint64
	This_addr  uint64
	Generation uint64
	
	Pointwidth uint64
	StartTime  uint64
	
	Foobar     uint64 "volatile"

	//Currently defined contents
	Addr   [KFACTOR]uint64
	Time   [KFACTOR]int64
	Count  [KFACTOR]uint64
	Min    [KFACTOR]float64
	Q1     [KFACTOR]float64
	Median [KFACTOR]float64
	Mean   [KFACTOR]float64
	Stdev  [KFACTOR]float64
	Q3     [KFACTOR]float64
	Max    [KFACTOR]float64
}
func (*Coreblock) GetDatablockType() BlockType {
	return Core
}

func DatablockGetBufferType(buf []byte) BlockType {
	t := binary.LittleEndian
	switch BlockType(t.Uint64(buf)) {
		case Vector:
		return Vector
		case Core:
		return Core
	}
	return Bad
}
func (v *Vectorblock) Serialize(dst []byte) {
	t := binary.LittleEndian
	t.PutUint64(dst[0:], v.Type)
	t.PutUint64(dst[8:], v.This_addr)
	t.PutUint64(dst[16:], v.Generation)
	
	idx := 24
	for i:=0; i<VSIZE; i++ {
		t.PutUint64(dst[idx:], uint64(v.Time[i]))
		idx += 8
	}
	for i:=0; i<VSIZE; i++ {
		t.PutUint64(dst[idx:], math.Float64bits(v.Value[i]))
		idx += 8
	}
}

func (v *Vectorblock) Deserialize(src []byte) {
	t := binary.LittleEndian
	v.Type	= t.Uint64(src[0:])
	v.This_addr = t.Uint64(src[8:])
	v.Generation = t.Uint64(src[16:])
	
	idx := 24
	for i:=0; i<VSIZE; i++ {
		v.Time[i] = int64(t.Uint64(src[idx:]))
		idx += 8
	}
	for i:=0; i<VSIZE; i++ {
		v.Value[i] = math.Float64frombits(t.Uint64(src[idx:]))
		idx += 8
	}
}
/**
 * The real function is meant to do things that might make the block
 * more friendly, like for instance doing delta compression so that the
 * blocks are more affected by ZRAM
 */
func (db *Coreblock) Serialize(dst []byte) {
	t := binary.LittleEndian
	//Address and generation first
	t.PutUint64(dst[0:], db.Type)
	t.PutUint64(dst[8:], db.This_addr)
	t.PutUint64(dst[16:], db.Generation)
	t.PutUint64(dst[24:], db.Pointwidth)
	t.PutUint64(dst[32:], db.StartTime)
	idx := 40
	//Now data
	for i := 0; i < KFACTOR; i++ {
		t.PutUint64(dst[idx:], db.Addr[i])
		idx += 8
	}
	for i := 0; i < KFACTOR; i++ {
		t.PutUint64(dst[idx:], uint64(db.Time[i]))
		idx += 8
	}
	for i := 0; i < KFACTOR; i++ {
		t.PutUint64(dst[idx:], db.Count[i])
		idx += 8
	}
	for i := 0; i < KFACTOR; i++ {
		t.PutUint64(dst[idx:], math.Float64bits(db.Min[i]))
		idx += 8
	}
	for i := 0; i < KFACTOR; i++ {
		t.PutUint64(dst[idx:], math.Float64bits(db.Q1[i]))
		idx += 8
	}
	for i := 0; i < KFACTOR; i++ {
		t.PutUint64(dst[idx:], math.Float64bits(db.Median[i]))
		idx += 8
	}
	for i := 0; i < KFACTOR; i++ {
		t.PutUint64(dst[idx:], math.Float64bits(db.Mean[i]))
		idx += 8
	}
	for i := 0; i < KFACTOR; i++ {
		t.PutUint64(dst[idx:], math.Float64bits(db.Stdev[i]))
		idx += 8
	}
	for i := 0; i < KFACTOR; i++ {
		t.PutUint64(dst[idx:], math.Float64bits(db.Q3[i]))
		idx += 8
	}
	for i := 0; i < KFACTOR; i++ {
		t.PutUint64(dst[idx:], math.Float64bits(db.Max[i]))
		idx += 8
	}
}
func (db *Coreblock) Deserialize(src []byte) {
	t := binary.LittleEndian
	//Address and generation first
	db.Type = t.Uint64(src[0:])
	db.This_addr = t.Uint64(src[8:])
	db.Generation = t.Uint64(src[16:])
	db.Pointwidth = t.Uint64(src[24:])
	db.StartTime = t.Uint64(src[32:])
	idx := 40
	//Now data
	for i := 0; i < KFACTOR; i++ {
		db.Addr[i] = t.Uint64(src[idx:])
		idx += 8
	}
	for i := 0; i < KFACTOR; i++ {
		db.Time[i] = int64(t.Uint64(src[idx:]))
		idx += 8
	}
	for i := 0; i < KFACTOR; i++ {
		db.Count[i] = t.Uint64(src[idx:])
		idx += 8
	}
	for i := 0; i < KFACTOR; i++ {
		db.Min[i] = math.Float64frombits(t.Uint64(src[idx:]))
		idx += 8
	}
	for i := 0; i < KFACTOR; i++ {
		db.Q1[i] = math.Float64frombits(t.Uint64(src[idx:]))
		idx += 8
	}
	for i := 0; i < KFACTOR; i++ {
		db.Median[i] = math.Float64frombits(t.Uint64(src[idx:]))
		idx += 8
	}
	for i := 0; i < KFACTOR; i++ {
		db.Mean[i] = math.Float64frombits(t.Uint64(src[idx:]))
		idx += 8
	}
	for i := 0; i < KFACTOR; i++ {
		db.Stdev[i] = math.Float64frombits(t.Uint64(src[idx:]))
		idx += 8
	}
	for i := 0; i < KFACTOR; i++ {
		db.Q3[i] = math.Float64frombits(t.Uint64(src[idx:]))
		idx += 8
	}
	for i := 0; i < KFACTOR; i++ {
		db.Max[i] = math.Float64frombits(t.Uint64(src[idx:]))
		idx += 8
	}
}
