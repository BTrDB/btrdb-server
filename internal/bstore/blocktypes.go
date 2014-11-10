package bstore

import (
	"code.google.com/p/go-uuid/uuid"
	"math"
)



type Superblock struct {
	uuid     uuid.UUID
	gen      uint64
	root     uint64
	unlinked bool
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

func (s *Superblock) Unlinked() bool {
	return s.unlinked
}

func NewSuperblock(id uuid.UUID) *Superblock {
	return &Superblock{
		uuid: id,
		gen:  1,
		root: 0,
	}
}

func (s *Superblock) Clone() *Superblock {
	return &Superblock{
		uuid: s.uuid,
		gen:  s.gen,
		root: s.root,
	}
}

type BlockType uint64

const (
	Vector BlockType = 1
	Core   BlockType = 2
	Bad    BlockType = 255
)

const FlagsMask uint8 = 3

type Datablock interface {
	GetDatablockType() BlockType
}

type Vectorblock struct {

	//Metadata, not copied on update
	Identifier uint64   "metadata"
	Generation uint64   "metadata"
	
	//Payload
	Len        uint8
	PointWidth uint8
	StartTime  int64
	Time       [VSIZE]int64
	Value      [VSIZE]float64
}

func (*Vectorblock) GetDatablockType() BlockType {
	return Vector
}

type Coreblock struct {

	//Metadata, not copied
	Identifier  uint64   "metadata"
	Generation uint64   "metadata"

	//Payload, copied
	PointWidth uint8
	StartTime  int64
	Addr       [KFACTOR]uint64
	Time       [KFACTOR]int64
	Count      [KFACTOR]uint64
	Min        [KFACTOR]float64
	Mean       [KFACTOR]float64
	Max        [KFACTOR]float64
	CGeneration [KFACTOR]uint64
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
	dst.Min = src.Min
	dst.Mean = src.Mean
	dst.Max = src.Max
	dst.CGeneration = src.CGeneration
}

func (src *Vectorblock) CopyInto(dst *Vectorblock) {
	dst.PointWidth = src.PointWidth
	dst.StartTime = src.StartTime
	dst.Len = src.Len
	dst.Time = src.Time
	dst.Value = src.Value
}

func DatablockGetBufferType(buf []byte) BlockType {
	switch (BlockType(buf[0] & FlagsMask)) {
	case Vector:
		return Vector
	case Core:
		return Core
	}
	return Bad
}


func (v *Vectorblock) Serialize(dst []byte) {
	/*
	t := binary.LittleEndian
	t.PutUint64(dst[0:], uint64(Vector))
	t.PutUint64(dst[8:], v.This_addr)
	t.PutUint64(dst[16:], v.Generation)
	t.PutUint64(dst[24:], uint64(v.Len))
	t.PutUint64(dst[32:], uint64(v.PointWidth))
	t.PutUint64(dst[40:], uint64(v.StartTime))
	t.PutUint64(dst[48:], uint64(v.MIBID))
	copy(dst[56:], v.UUID[:])

	idx := vb_payload_offset
	for i := 0; i < VSIZE; i++ {
		t.PutUint64(dst[idx:], uint64(v.Time[i]))
		idx += 8
	}
	for i := 0; i < VSIZE; i++ {
		t.PutUint64(dst[idx:], math.Float64bits(v.Value[i]))
		idx += 8
	}*/
}

func (v *Vectorblock) Deserialize(src []byte) {
	/*
	t := binary.LittleEndian
	v.This_addr = t.Uint64(src[8:])
	v.Generation = t.Uint64(src[16:])
	v.Len = int(t.Uint64(src[24:]))
	v.PointWidth = uint8(t.Uint64(src[32:]))
	v.StartTime = int64(t.Uint64(src[40:]))
	v.MIBID = t.Uint64(src[48:])
	copy(v.UUID[:], src[56:72])

	idx := vb_payload_offset
	for i := 0; i < VSIZE; i++ {
		v.Time[i] = int64(t.Uint64(src[idx:]))
		idx += 8
	}
	for i := 0; i < VSIZE; i++ {
		v.Value[i] = math.Float64frombits(t.Uint64(src[idx:]))
		idx += 8
	}
	*/
}

/**
 * The real function is meant to do things that might make the block
 * more friendly, like for instance doing delta compression so that the
 * blocks are more affected by ZRAM
 */
func (db *Coreblock) Serialize(dst []byte) {
	/*
	t := binary.LittleEndian
	//Address and generation first
	t.PutUint64(dst[0:], uint64(Core))
	t.PutUint64(dst[8:], db.This_addr)
	t.PutUint64(dst[16:], db.Generation)
	t.PutUint64(dst[24:], uint64(db.PointWidth))
	t.PutUint64(dst[32:], uint64(db.StartTime))
	t.PutUint64(dst[40:], db.MIBID)
	copy(dst[48:], db.UUID[:])

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
	for i := 0; i < KFACTOR; i++ {
		t.PutUint64(dst[idx:], db.CGeneration[i])
		idx += 8
	}
	*/
}
func (db *Coreblock) Deserialize(src []byte) {
	/*
	t := binary.LittleEndian
	//Address and generation first
	db.This_addr = t.Uint64(src[8:])
	db.Generation = t.Uint64(src[16:])
	db.PointWidth = uint8(t.Uint64(src[24:]))
	db.StartTime = int64(t.Uint64(src[32:]))
	db.MIBID = t.Uint64(src[40:])
	copy(db.UUID[:], src[48:64])

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
	for i := 0; i < KFACTOR; i++ {
		db.CGeneration[i] = t.Uint64(src[idx:])
		idx += 8
	}
	*/
}

//These functions allow us to read/write the packed numbers in the datablocks
//These are huffman encoded in big endian
// 0xxx xxxx           7  0x00
// 10xx xxxx +1        14 0x80
// 1100 xxxx +2        20 0xC0
// 1101 xxxx +3        28 0xD0
// 1110 xxxx +4        36 0xE0
// 1111 00xx +5        42 0xF0
// 1111 01xx +6        50 0xF4
// 1111 10xx +7        58 0xF8
// 1111 1100 +8        64 0xFC
func writeUnsignedHuff(dst []byte, val uint64) int {
	i := 0
	var do_rest func (n uint8)()
	do_rest = func(n uint8) {
		if n == 0 {
			return
		}
		dst[i] = byte((val >> ((n-1)*8))& 0xFF)
		i++
		do_rest(n-1)
	}
	if val < (1<<7) {
		dst[i] = byte(val)
		i++
	} else if val < (1<<14) {
		dst[i] = byte(0x80 | val >> 8)
		i++
		do_rest(1)
	} else if val < (1<<20) {
		dst[i] = byte(0xC0 | val >> 16)
		i++
		do_rest(2)
	} else if val < (1<<28) {
		dst[i] = byte(0xD0 | val >> 24)
		i++
		do_rest(3)
	} else if val < (1<<36) {
		dst[i] = byte(0xE0 | val >> 32)
		i++
		do_rest(4)
	} else if val < (1<<42) {
		dst[i] = byte(0xF0 | val >> 40)
		i++
		do_rest(5)
	} else if val < (1<<50) {
		dst[i] = byte(0xF4 | val >> 48)
		i++
		do_rest(6)
	} else if val < (1<<58) {
		dst[i] = byte(0xF8 | val >> 56)
		i++
		do_rest(7)
	} else {
		dst[i] = 0xFC
		i++
		do_rest(8)
	}
	return i
}
func writeSignedHuff(dst []byte, val int64) int {
	if val < 0 {
		return writeUnsignedHuff(dst, (uint64(-val) << 1 | 1))
	} else {
		return writeUnsignedHuff(dst, uint64(val)<<1)
	}
}
func readUnsignedHuff(src []byte) uint64 {
	var rv uint64
	i := 1
	var do_rest func (n uint8)()
	do_rest = func(n uint8) {
		if n == 0 {
			return
		}
		rv <<= 8
		rv |= uint64(src[i])
		i++
		do_rest(n-1)
	}
	if src[0] >  0xFC {
		log.Panicf("This huffman symbol is reserved: +v",src[0])
	} else if src[0] == 0xFC {
		do_rest(8)
	} else if src[0] >= 0xF8 {
		rv = uint64(src[0] &  0x03)
		do_rest(7)
	} else if src[0] >= 0xF4 {
		rv =  uint64(src[0] & 0x03)
		do_rest(6)
	} else if src[0] >= 0xF0 {
		rv = uint64(src[0] &  0x03)
		do_rest(5)
	} else if src[0] >= 0xE0 {
		rv = uint64(src[0] &  0x0F)
		do_rest(4)
	} else if src[0] >= 0xD0 {
		rv = uint64(src[0] &  0x0F)
		do_rest(3)
	} else if src[0] >= 0xC0 {
		rv = uint64(src[0] &  0x0F)
		do_rest(2)
	} else if src[0] >= 0x80 {
		rv = uint64(src[0] &  0x3F)
		do_rest(1)
	} else {
		rv = uint64(src[0] & 0x7F)
	}
	return rv
}
func readSignedHuff(src []byte) int64 {
	v := readUnsignedHuff(src)
	s := v&1
	v >>= 1
	if s == 1 {
		return -int64(v)
	}
	return int64(v)
}
//This composes a float into a weird representation that was empirically determined to be
//ideal for compression of Quasar streams.
//First we split out the sign, exponent and mantissa from the float
//Then we reverse the bytes in the mantissa (bits are better but slower)
//Then we left shift it and stick the sign bit as the LSB
//The result is the (unsigned) exponent and the mantissa-sortof-thingy
func decompose(val float64) (e uint16, m uint64) {
	iv := math.Float64bits(val)
	s := iv >> 63
	exp := (iv >> 52) & 1023
	iv = iv & ((1<<52) - 1)
	//Take the bottom 7 bytes and reverse them. Top byte is left zero
	//                 . . . . . .
	m  = ((iv & 0x00000000000000FF) << (6*8) |
	      (iv & 0x000000000000FF00) << (4*8) |
	      (iv & 0x0000000000FF0000) << (2*8) |
	      (iv & 0x00000000FF000000) 		 |
	      (iv & 0x000000FF00000000) >> (2*8) |
	      (iv & 0x0000FF0000000000) >> (4*8) |
	      (iv & 0x00FF000000000000) >> (6*8))
	m <<= 1
	m |= s
	e = uint16(exp)
	return
}

func recompose(e uint16, m uint64) float64 {
	s := m&1
	m >>= 1
	iv := ((m & 0x00000000000000FF) << (6*8) |
	       (m & 0x000000000000FF00) << (4*8) |
	       (m & 0x0000000000FF0000) << (2*8) |
	       (m & 0x00000000FF000000) 		 |
	       (m & 0x000000FF00000000) >> (2*8) |
	       (m & 0x0000FF0000000000) >> (4*8) |
	       (m & 0x00FF000000000000) >> (6*8))
	iv |= uint64(e) << 52
	iv |= s << 63
	return math.Float64frombits(iv)
}
