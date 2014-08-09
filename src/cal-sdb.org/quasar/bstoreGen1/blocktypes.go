package bstoreGen1

import (
	"encoding/binary"
	"math"
	"code.google.com/p/go-uuid/uuid"
)

//We are aiming for datablocks to be 8Kbytes
const DBSIZE = 8192

type Superblock struct {
	uuid uuid.UUID
	gen  uint64
	root uint64
}

func (s *Superblock) Gen() uint64 {
	return s.gen
}

func (s *Superblock) Root() uint64 {
	return s.root
}

func (s *Superblock) Uuid() uuid.UUID {
	return s.uuid
}

func NewSuperblock(id uuid.UUID) *Superblock {
	return &Superblock {
		uuid:id,
		gen:1,
		root:0,
	}
}

func (s *Superblock) Clone() *Superblock {
	return &Superblock {
		uuid:s.uuid,
		gen:s.gen,
		root:s.root,
	}
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

const vb_payload_offset = 512
type Vectorblock struct {
	
	//Metadata, not copied
	This_addr  uint64   "metadata"
	Generation uint64	"metadata"
	MIBID	   uint64	"metadata"
	
	//Payload
	Len		    int
	PointWidth  uint8
	StartTime	int64
	Time       [VSIZE]int64
	Value	   [VSIZE]float64
}

func (*Vectorblock) GetDatablockType() BlockType {
	return Vector
}

const cb_payload_offset = 512
type Coreblock struct {
	
	//Metadata, not copied
	This_addr  uint64	"metadata"
	Generation uint64	"metadata"
	MIBID	   uint64   "metadata"
	
	//Payload, copied
	PointWidth uint8
	StartTime  int64
	Addr   [KFACTOR]uint64
	Time   [KFACTOR]int64
	Count  [KFACTOR]uint64
	Flags  [KFACTOR]uint64
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

//Copy a core block, only copying the payload, not the metadata
func (src *Coreblock) CopyInto(dst *Coreblock) {
	dst.PointWidth = src.PointWidth
	dst.StartTime = src.StartTime
	dst.Addr = src.Addr
	dst.Time = src.Time
	dst.Count = src.Count
	dst.Flags = src.Flags
	dst.Min = src.Min
	dst.Q1 = src.Q1
	dst.Median = src.Median
	dst.Mean = src.Mean
	dst.Stdev = src.Stdev
	dst.Q3 = src.Q3
	dst.Max = src.Max
}

func (src *Vectorblock) CopyInto(dst *Vectorblock) {
	dst.PointWidth = src.PointWidth
	dst.StartTime = src.StartTime
	dst.Len = src.Len
	dst.Time = src.Time
	dst.Value = src.Value
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
	t.PutUint64(dst[0:], uint64(Vector))
	t.PutUint64(dst[8:], v.This_addr)
	t.PutUint64(dst[16:], v.Generation)
	t.PutUint64(dst[24:], uint64(v.Len))
	t.PutUint64(dst[32:], uint64(v.PointWidth))
	t.PutUint64(dst[40:], uint64(v.StartTime))
	t.PutUint64(dst[48:], uint64(v.MIBID))
	idx := vb_payload_offset
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
	v.This_addr = t.Uint64(src[8:])
	v.Generation = t.Uint64(src[16:])
	v.Len = int(t.Uint64(src[24:]))
	v.PointWidth = uint8(t.Uint64(src[32:]))
	v.StartTime = int64(t.Uint64(src[40:]))
	v.MIBID = t.Uint64(src[48:])
	idx := vb_payload_offset
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
	t.PutUint64(dst[0:], uint64(Core))
	t.PutUint64(dst[8:], db.This_addr)
	t.PutUint64(dst[16:], db.Generation)
	t.PutUint64(dst[24:], uint64(db.PointWidth))
	t.PutUint64(dst[32:], uint64(db.StartTime))
	t.PutUint64(dst[40:], db.MIBID)
	idx := cb_payload_offset
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
		t.PutUint64(dst[idx:], db.Flags[i])
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
	db.This_addr = t.Uint64(src[8:])
	db.Generation = t.Uint64(src[16:])
	db.PointWidth = uint8(t.Uint64(src[24:]))
	db.StartTime = int64(t.Uint64(src[32:]))
	db.MIBID = t.Uint64(src[40:])
	idx := cb_payload_offset
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
		db.Flags[i] = t.Uint64(src[idx:])
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
