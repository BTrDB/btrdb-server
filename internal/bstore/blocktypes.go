package bstore

import (
	"encoding/binary"
	"math"
	"time"

	"github.com/pborman/uuid"
)

type Superblock struct {
	uuid     uuid.UUID
	gen      uint64
	root     uint64
	walltime int64
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
	return &Superblock{
		uuid:     id,
		gen:      1,
		root:     0,
		walltime: time.Now().UnixNano(),
	}
}

func (s *Superblock) CloneInc() *Superblock {
	return &Superblock{
		uuid:     s.uuid,
		gen:      s.gen + 1,
		root:     s.root,
		walltime: time.Now().UnixNano(),
	}
}

func DeserializeSuperblock(id uuid.UUID, gen uint64, arr []byte) *Superblock {
	return &Superblock{
		uuid:     id,
		gen:      gen,
		root:     binary.LittleEndian.Uint64(arr),
		walltime: int64(binary.LittleEndian.Uint64(arr[8:])),
	}
}

func (s *Superblock) Serialize() []byte {
	rv := make([]byte, 16)
	binary.LittleEndian.PutUint64(rv[:8], s.root)
	binary.LittleEndian.PutUint64(rv[8:], uint64(s.walltime))
	return rv
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

// The leaf datablock type. The tags allow unit tests
// to work out if clone / serdes are working properly
// metadata is not copied when a node is cloned
// implicit is not serialised
type Vectorblock struct {

	//Metadata, not copied on clone
	Identifier uint64 "metadata,implicit"
	Generation uint64 "metadata,implicit"

	//Payload, copied on clone
	Len        uint16
	PointWidth uint8 "implicit"
	StartTime  int64 "implicit"
	Time       [VSIZE]int64
	Value      [VSIZE]float64
}

type Coreblock struct {

	//Metadata, not copied
	Identifier uint64 "metadata,implicit"
	Generation uint64 "metadata,implicit"

	//Payload, copied
	PointWidth  uint8 "implicit"
	StartTime   int64 "implicit"
	Addr        [KFACTOR]uint64
	Count       [KFACTOR]uint64
	Min         [KFACTOR]float64
	Mean        [KFACTOR]float64
	Max         [KFACTOR]float64
	CGeneration [KFACTOR]uint64
}

func (*Vectorblock) GetDatablockType() BlockType {
	return Vector
}

func (*Coreblock) GetDatablockType() BlockType {
	return Core
}

//Copy a core block, only copying the payload, not the metadata
func (src *Coreblock) CopyInto(dst *Coreblock) {
	dst.PointWidth = src.PointWidth
	dst.StartTime = src.StartTime
	dst.Addr = src.Addr
	//dst.Time = src.Time
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
	switch BlockType(buf[0]) {
	case Vector:
		return Vector
	case Core:
		return Core
	}
	return Bad
}

// The current algorithm is as follows:
// entry 0: absolute time and value
// entry 1: delta time and value since 0
// entry 2: delta since delta 1
// entry 3: delta from average delta (1+2)
// enrty 4+ delta from average delta (n-1, n-2, n-3)

func (v *Vectorblock) Serialize(dst []byte) []byte {
	idx := 3
	dst[0] = byte(Vector)
	dst[1] = byte(v.Len)
	dst[2] = byte(v.Len >> 8)

	if v.Len == 0 {
		return dst[:idx]
	}
	//First values are written in full
	e, m := decompose(v.Value[0])
	idx += writeUnsignedHuff(dst[idx:], m)
	idx += writeUnsignedHuff(dst[idx:], uint64(e))

	//So we are taking a gamble here: I think I will never have negative times. If I do,
	//this will use 9 bytes for every time. But I won't.
	t := v.Time[0]
	idx += writeUnsignedHuff(dst[idx:], uint64(t))
	if v.Len == 1 {
		return dst[:idx]
	}

	const delta_depth = 3
	hist_deltas_t := make([]int64, delta_depth)
	hist_deltas_e := make([]int64, delta_depth)
	hist_deltas_m := make([]int64, delta_depth)
	delta_idx := 0
	num_deltas := 0

	em1 := int64(e)
	mm1 := int64(m)
	tm1 := t
	for i := 1; i < int(v.Len); i++ {
		var deltas int
		if num_deltas > delta_depth {
			deltas = delta_depth
		} else {
			deltas = num_deltas
		}
		var e, m int64
		tmpe, tmpm := decompose(v.Value[i])
		e = int64(tmpe)
		m = int64(tmpm)
		t := v.Time[i]

		//Calculate the delta for this record
		dt := t - tm1
		de := e - em1
		dm := m - mm1

		//Calculate average deltas
		var dt_total int64 = 0
		var dm_total int64 = 0
		var de_total int64 = 0
		for d := 0; d < deltas; d++ {
			dt_total += hist_deltas_t[d]
			dm_total += hist_deltas_m[d]
			de_total += hist_deltas_e[d]
		}
		var adt, ade, adm int64 = 0, 0, 0
		if deltas != 0 {
			adt = dt_total / int64(deltas)
			ade = de_total / int64(deltas)
			adm = dm_total / int64(deltas)
		}
		//Calculate the delta delta
		ddt := dt - adt
		dde := de - ade
		ddm := dm - adm

		//Add in the delta for this record
		hist_deltas_t[delta_idx] = dt
		hist_deltas_e[delta_idx] = de
		hist_deltas_m[delta_idx] = dm
		delta_idx++
		if delta_idx == delta_depth {
			delta_idx = 0
		}
		num_deltas++

		//Encode dde nz and ddt nz into ddm
		ddm <<= 2
		if dde != 0 {
			ddm |= 2
		}
		if ddt != 0 {
			ddm |= 1
		}

		//Write it out
		idx += writeSignedHuff(dst[idx:], ddm)
		if dde != 0 {
			idx += writeSignedHuff(dst[idx:], dde)
		}
		if ddt != 0 {
			idx += writeSignedHuff(dst[idx:], ddt)
		}

		em1 = e
		tm1 = t
		mm1 = m
	}
	return dst[:idx]
}

func (v *Vectorblock) Deserialize(src []byte) {
	blocktype := src[0]
	if BlockType(blocktype) != Vector {
		lg.Panicf("This is not a vector block")
	}

	v.Len = uint16(src[1]) + (uint16(src[2]) << 8)
	length := int(v.Len)
	idx := 3

	m, l, _ := readUnsignedHuff(src[idx:])
	idx += l
	e, l, _ := readUnsignedHuff(src[idx:])
	idx += l
	t, l, _ := readUnsignedHuff(src[idx:])
	idx += l
	v.Time[0] = int64(t)
	v.Value[0] = recompose(uint16(e), uint64(m))

	//Keep delta history
	const delta_depth = 3
	hist_deltas_t := make([]int64, delta_depth)
	hist_deltas_e := make([]int64, delta_depth)
	hist_deltas_m := make([]int64, delta_depth)
	delta_idx := 0
	num_deltas := 0

	mm1 := int64(m)
	em1 := int64(e)
	tm1 := int64(t)
	for i := 1; i < length; i++ {
		//How many deltas do we have
		var deltas int
		if num_deltas > delta_depth {
			deltas = delta_depth
		} else {
			deltas = num_deltas
		}

		//Calculate average deltas
		var dt_total int64 = 0
		var dm_total int64 = 0
		var de_total int64 = 0
		for d := 0; d < deltas; d++ {
			dt_total += hist_deltas_t[d]
			dm_total += hist_deltas_m[d]
			de_total += hist_deltas_e[d]
		}
		var adt, ade, adm int64 = 0, 0, 0
		if deltas != 0 {
			adt = dt_total / int64(deltas)
			ade = de_total / int64(deltas)
			adm = dm_total / int64(deltas)
		}
		//Read the dd's
		ddm, l, _ := readSignedHuff(src[idx:])
		idx += l
		var dde, ddt int64 = 0, 0
		if ddm&2 != 0 {
			//log.Warning("re")
			dde, l, _ = readSignedHuff(src[idx:])
			idx += l
		}
		if ddm&1 != 0 {
			//log.Warning("rt")
			ddt, l, _ = readSignedHuff(src[idx:])
			idx += l
		}
		ddm >>= 2
		//Convert dd's to d's
		dm := ddm + adm
		dt := ddt + adt
		de := dde + ade

		//Save the deltas in the history
		hist_deltas_t[delta_idx] = dt
		hist_deltas_m[delta_idx] = dm
		hist_deltas_e[delta_idx] = de
		delta_idx++
		if delta_idx == delta_depth {
			delta_idx = 0
		}
		num_deltas++

		//Save values
		e := em1 + de
		m := mm1 + dm
		v.Time[i] = tm1 + dt
		v.Value[i] = recompose(uint16(e), uint64(m))
		em1 += de
		mm1 += dm
		tm1 += dt
	}
}

func (c *Coreblock) Serialize(dst []byte) []byte {
	/*
		Addr       delta-delta / abszero
		Count      delta +isnz(cgen)
		CGeneration delta-delta
		Mean       delta-delta (mantissa contains isnz(e))
		Min        delta-delta (mantissa contains isnz(e))
		Max        delta-delta (mantissa contains isnz(e))

		TL;DR the code is the documentation MWAHAHAHA
	*/

	idx := 1
	dst[0] = byte(Core)

	const delta_depth = 3

	deltadeltarizer := func(maxdepth int) func(value int64) int64 {
		hist_delta := make([]int64, maxdepth)
		var depth int = 0
		insidx := 0
		var last_value int64
		dd := func(value int64) int64 {
			var total_dt int64 = 0
			for i := 0; i < depth; i++ {
				total_dt += hist_delta[i]
			}
			var avg_dt int64 = 0
			if depth > 0 {
				avg_dt = total_dt / int64(depth)
			}
			curdelta := value - last_value
			last_value = value
			ddelta := curdelta - avg_dt
			hist_delta[insidx] = curdelta
			insidx = (insidx + 1) % maxdepth
			depth += 1
			if depth > maxdepth {
				depth = maxdepth
			}
			return ddelta
		}
		return dd
	}
	dd_addr := deltadeltarizer(delta_depth)
	dd_cgen := deltadeltarizer(delta_depth)
	dd_count := deltadeltarizer(delta_depth)
	dd_mean_m := deltadeltarizer(delta_depth)
	dd_mean_e := deltadeltarizer(delta_depth)
	dd_min_m := deltadeltarizer(delta_depth)
	dd_min_e := deltadeltarizer(delta_depth)
	dd_max_m := deltadeltarizer(delta_depth)
	dd_max_e := deltadeltarizer(delta_depth)

	//Look for bottomable idx
	bottomidx := -1
	for i := KFACTOR - 1; i >= 0; i-- {
		if c.Addr[i] == 0 && c.CGeneration[i] == 0 {
			bottomidx = i
		} else {
			break
		}
	}
	for i := 0; i < KFACTOR; i++ {
		if i == bottomidx {
			idx += writeFullZero(dst[idx:])
			break
		}
		if c.Addr[i] == 0 {
			idx += writeAbsZero(dst[idx:])
			idx += writeSignedHuff(dst[idx:], dd_cgen(int64(c.CGeneration[i])))
		} else {
			idx += writeSignedHuff(dst[idx:], dd_addr(int64(c.Addr[i])))

			min_e, min_m := decompose(c.Min[i])
			min_m_dd := dd_min_m(int64(min_m))
			min_e_dd := dd_min_e(int64(min_e))
			min_m_dd <<= 1
			if min_e_dd != 0 {
				min_m_dd |= 1
			}

			mean_e, mean_m := decompose(c.Mean[i])
			mean_m_dd := dd_mean_m(int64(mean_m))
			mean_e_dd := dd_mean_e(int64(mean_e))
			mean_m_dd <<= 1
			if mean_e_dd != 0 {
				mean_m_dd |= 1
			}

			max_e, max_m := decompose(c.Max[i])
			max_m_dd := dd_max_m(int64(max_m))
			max_e_dd := dd_max_e(int64(max_e))
			max_m_dd <<= 1
			if max_e_dd != 0 {
				max_m_dd |= 1
			}

			cgen_dd := dd_cgen(int64(c.CGeneration[i]))

			cnt := dd_count(int64(c.Count[i]))
			cnt <<= 1
			if cgen_dd != 0 {
				cnt |= 1
			}
			idx += writeSignedHuff(dst[idx:], cnt)
			if cgen_dd != 0 {
				idx += writeSignedHuff(dst[idx:], cgen_dd)
			}
			idx += writeSignedHuff(dst[idx:], min_m_dd)
			if min_e_dd != 0 {
				idx += writeSignedHuff(dst[idx:], min_e_dd)
			}
			idx += writeSignedHuff(dst[idx:], mean_m_dd)
			if mean_e_dd != 0 {
				idx += writeSignedHuff(dst[idx:], mean_e_dd)
			}
			idx += writeSignedHuff(dst[idx:], max_m_dd)
			if max_e_dd != 0 {
				idx += writeSignedHuff(dst[idx:], max_e_dd)
			}
		}
		//log.Warning("Finished SER %v, idx is %v", i, idx)
	}
	return dst[:idx]
}

func (c *Coreblock) Deserialize(src []byte) {
	//check 0 for id
	if src[0] != byte(Core) {
		lg.Panic("This is not a core block")
	}
	idx := 1
	dedeltadeltarizer := func(maxdepth int) func(dd int64) int64 {
		hist_delta := make([]int64, maxdepth)
		depth := 0
		insidx := 0
		var last_value int64 = 0
		decode := func(dd int64) int64 {
			var total_dt int64 = 0
			for i := 0; i < depth; i++ {
				total_dt += hist_delta[i]
			}
			var avg_dt int64 = 0
			if depth > 0 {
				avg_dt = total_dt / int64(depth)
			}
			curdelta := avg_dt + dd
			curvalue := last_value + curdelta
			last_value = curvalue
			hist_delta[insidx] = curdelta
			insidx = (insidx + 1) % maxdepth
			depth += 1
			if depth > maxdepth {
				depth = maxdepth
			}
			return last_value
		}
		return decode
	}

	const delta_depth = 3
	dd_addr := dedeltadeltarizer(delta_depth)
	dd_cgen := dedeltadeltarizer(delta_depth)
	dd_count := dedeltadeltarizer(delta_depth)
	dd_mean_m := dedeltadeltarizer(delta_depth)
	dd_mean_e := dedeltadeltarizer(delta_depth)
	dd_min_m := dedeltadeltarizer(delta_depth)
	dd_min_e := dedeltadeltarizer(delta_depth)
	dd_max_m := dedeltadeltarizer(delta_depth)
	dd_max_e := dedeltadeltarizer(delta_depth)

	i := 0
	for ; i < KFACTOR; i++ {

		//Get addr
		addr_dd, used, bottom := readSignedHuff(src[idx:])
		idx += used
		if bottom == ABSZERO {
			c.Addr[i] = 0
			c.Count[i] = 0
			//min/mean/max are undefined
			//Still have to decode cgen
			cgen_dd, used, _ := readSignedHuff(src[idx:])
			idx += used
			cgen := uint64(dd_cgen(cgen_dd))
			c.CGeneration[i] = cgen
		} else if bottom == FULLZERO {
			break
		} else {
			//Real value
			c.Addr[i] = uint64(dd_addr(addr_dd))

			cnt_dd, used, _ := readSignedHuff(src[idx:])
			idx += used

			var cgen_dd int64 = 0
			if cnt_dd&1 != 0 {
				cgen_dd, used, _ = readSignedHuff(src[idx:])
				idx += used
			}
			cnt_dd >>= 1
			c.CGeneration[i] = uint64(dd_cgen(cgen_dd))
			c.Count[i] = uint64(dd_count(cnt_dd))

			min_m_dd, used, _ := readSignedHuff(src[idx:])
			idx += used
			var min_e_dd int64
			if min_m_dd&1 != 0 {
				min_e_dd, used, _ = readSignedHuff(src[idx:])
				idx += used
			} else {
				min_e_dd = 0
			}
			min_m_dd >>= 1
			c.Min[i] = recompose(uint16(dd_min_e(min_e_dd)), uint64(dd_min_m(min_m_dd)))

			mean_m_dd, used, _ := readSignedHuff(src[idx:])
			idx += used
			var mean_e_dd int64
			if mean_m_dd&1 != 0 {
				mean_e_dd, used, _ = readSignedHuff(src[idx:])
				idx += used
			} else {
				mean_e_dd = 0
			}
			mean_m_dd >>= 1
			c.Mean[i] = recompose(uint16(dd_mean_e(mean_e_dd)), uint64(dd_mean_m(mean_m_dd)))

			max_m_dd, used, _ := readSignedHuff(src[idx:])
			idx += used
			var max_e_dd int64
			if max_m_dd&1 != 0 {
				max_e_dd, used, _ = readSignedHuff(src[idx:])
				idx += used
			} else {
				max_e_dd = 0
			}
			max_m_dd >>= 1
			c.Max[i] = recompose(uint16(dd_max_e(max_e_dd)), uint64(dd_max_m(max_m_dd)))
		}
		//log.Warning("Finishing deser idx %v, idx is %v",i, idx)
	}

	//Clear out from a FULLZERO
	for ; i < KFACTOR; i++ {
		c.Addr[i] = 0
		c.Count[i] = 0
		c.CGeneration[i] = 0

	}
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
// 1111 1101 +0        ABSZERO (special symbol) 0xFD
// 1111 1110 +0        FULLZERO (special symbol) 0xFE
const VALUE = 0
const ABSZERO = 1
const FULLZERO = 2

func writeUnsignedHuff(dst []byte, val uint64) int {
	//log.Warning("wuh called dstlen %v",len(dst))
	i := 0
	var do_rest func(n uint8)
	do_rest = func(n uint8) {
		if n == 0 {
			return
		}
		dst[i] = byte((val >> ((n - 1) * 8)) & 0xFF)
		i++
		do_rest(n - 1)
	}
	if val < (1 << 7) {
		dst[i] = byte(val)
		i++
	} else if val < (1 << 14) {
		dst[i] = byte(0x80 | val>>8)
		i++
		do_rest(1)
	} else if val < (1 << 20) {
		dst[i] = byte(0xC0 | val>>16)
		i++
		do_rest(2)
	} else if val < (1 << 28) {
		dst[i] = byte(0xD0 | val>>24)
		i++
		do_rest(3)
	} else if val < (1 << 36) {
		dst[i] = byte(0xE0 | val>>32)
		i++
		do_rest(4)
	} else if val < (1 << 42) {
		dst[i] = byte(0xF0 | val>>40)
		i++
		do_rest(5)
	} else if val < (1 << 50) {
		dst[i] = byte(0xF4 | val>>48)
		i++
		do_rest(6)
	} else if val < (1 << 58) {
		dst[i] = byte(0xF8 | val>>56)
		i++
		do_rest(7)
	} else {
		dst[i] = 0xFC
		i++
		do_rest(8)
	}
	return i
}
func writeAbsZero(dst []byte) int {
	dst[0] = 0xFD
	return 1
}
func writeFullZero(dst []byte) int {
	dst[0] = 0xFE
	return 1
}
func writeSignedHuff(dst []byte, val int64) int {
	if val < 0 {
		return writeUnsignedHuff(dst, (uint64(-val)<<1 | 1))
	} else {
		return writeUnsignedHuff(dst, uint64(val)<<1)
	}
}
func readUnsignedHuff(src []byte) (uint64, int, int) {
	var rv uint64
	i := 1
	var do_rest func(n uint8)
	do_rest = func(n uint8) {
		if n == 0 {
			return
		}
		rv <<= 8
		rv |= uint64(src[i])
		i++
		do_rest(n - 1)
	}
	if src[0] > 0xFE {
		lg.Panicf("This huffman symbol is reserved: +v", src[0])
	} else if src[0] == 0xFD {
		return 0, 1, ABSZERO
	} else if src[0] == 0xFE {
		return 0, 1, FULLZERO
	} else if src[0] == 0xFC {
		do_rest(8)
	} else if src[0] >= 0xF8 {
		rv = uint64(src[0] & 0x03)
		do_rest(7)
	} else if src[0] >= 0xF4 {
		rv = uint64(src[0] & 0x03)
		do_rest(6)
	} else if src[0] >= 0xF0 {
		rv = uint64(src[0] & 0x03)
		do_rest(5)
	} else if src[0] >= 0xE0 {
		rv = uint64(src[0] & 0x0F)
		do_rest(4)
	} else if src[0] >= 0xD0 {
		rv = uint64(src[0] & 0x0F)
		do_rest(3)
	} else if src[0] >= 0xC0 {
		rv = uint64(src[0] & 0x0F)
		do_rest(2)
	} else if src[0] >= 0x80 {
		rv = uint64(src[0] & 0x3F)
		do_rest(1)
	} else {
		rv = uint64(src[0] & 0x7F)
	}
	return rv, i, VALUE
}
func readSignedHuff(src []byte) (int64, int, int) {
	v, l, bv := readUnsignedHuff(src)
	if bv != VALUE {
		return 0, 1, bv
	}
	s := v & 1
	v >>= 1
	if s == 1 {
		return -int64(v), l, VALUE
	}
	return int64(v), l, VALUE
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
	exp := (iv >> 52) & 2047
	iv = iv & ((1 << 52) - 1)
	//Take the bottom 7 bytes and reverse them. Top byte is left zero
	//                 . . . . . .
	m = ((iv&0x00000000000000FF)<<(6*8) |
		(iv&0x000000000000FF00)<<(4*8) |
		(iv&0x0000000000FF0000)<<(2*8) |
		(iv & 0x00000000FF000000) |
		(iv&0x000000FF00000000)>>(2*8) |
		(iv&0x0000FF0000000000)>>(4*8) |
		(iv&0x00FF000000000000)>>(6*8))
	e = (uint16(exp) << 1) | uint16(s)
	return
}

func recompose(e uint16, m uint64) float64 {
	s := e & 1
	e >>= 1
	iv := ((m&0x00000000000000FF)<<(6*8) |
		(m&0x000000000000FF00)<<(4*8) |
		(m&0x0000000000FF0000)<<(2*8) |
		(m & 0x00000000FF000000) |
		(m&0x000000FF00000000)>>(2*8) |
		(m&0x0000FF0000000000)>>(4*8) |
		(m&0x00FF000000000000)>>(6*8))
	iv |= uint64(e) << 52
	iv |= uint64(s) << 63
	return math.Float64frombits(iv)
}
