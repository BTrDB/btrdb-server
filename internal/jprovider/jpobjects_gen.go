package jprovider

// NOTE: THIS FILE WAS PRODUCED BY THE
// MSGP CODE GENERATION TOOL (github.com/tinylib/msgp)
// DO NOT EDIT

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *JournalRecord) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zbai uint32
	zbai, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	for zbai > 0 {
		zbai--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "UUID":
			z.UUID, err = dc.ReadBytes(z.UUID)
			if err != nil {
				return
			}
		case "MajorVersion":
			z.MajorVersion, err = dc.ReadUint64()
			if err != nil {
				return
			}
		case "MicroVersion":
			z.MicroVersion, err = dc.ReadUint32()
			if err != nil {
				return
			}
		case "Times":
			var zcmr uint32
			zcmr, err = dc.ReadArrayHeader()
			if err != nil {
				return
			}
			if cap(z.Times) >= int(zcmr) {
				z.Times = (z.Times)[:zcmr]
			} else {
				z.Times = make([]int64, zcmr)
			}
			for zxvk := range z.Times {
				z.Times[zxvk], err = dc.ReadInt64()
				if err != nil {
					return
				}
			}
		case "Values":
			var zajw uint32
			zajw, err = dc.ReadArrayHeader()
			if err != nil {
				return
			}
			if cap(z.Values) >= int(zajw) {
				z.Values = (z.Values)[:zajw]
			} else {
				z.Values = make([]float64, zajw)
			}
			for zbzg := range z.Values {
				z.Values[zbzg], err = dc.ReadFloat64()
				if err != nil {
					return
				}
			}
		default:
			err = dc.Skip()
			if err != nil {
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z *JournalRecord) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 5
	// write "UUID"
	err = en.Append(0x85, 0xa4, 0x55, 0x55, 0x49, 0x44)
	if err != nil {
		return err
	}
	err = en.WriteBytes(z.UUID)
	if err != nil {
		return
	}
	// write "MajorVersion"
	err = en.Append(0xac, 0x4d, 0x61, 0x6a, 0x6f, 0x72, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e)
	if err != nil {
		return err
	}
	err = en.WriteUint64(z.MajorVersion)
	if err != nil {
		return
	}
	// write "MicroVersion"
	err = en.Append(0xac, 0x4d, 0x69, 0x63, 0x72, 0x6f, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e)
	if err != nil {
		return err
	}
	err = en.WriteUint32(z.MicroVersion)
	if err != nil {
		return
	}
	// write "Times"
	err = en.Append(0xa5, 0x54, 0x69, 0x6d, 0x65, 0x73)
	if err != nil {
		return err
	}
	err = en.WriteArrayHeader(uint32(len(z.Times)))
	if err != nil {
		return
	}
	for zxvk := range z.Times {
		err = en.WriteInt64(z.Times[zxvk])
		if err != nil {
			return
		}
	}
	// write "Values"
	err = en.Append(0xa6, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x73)
	if err != nil {
		return err
	}
	err = en.WriteArrayHeader(uint32(len(z.Values)))
	if err != nil {
		return
	}
	for zbzg := range z.Values {
		err = en.WriteFloat64(z.Values[zbzg])
		if err != nil {
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *JournalRecord) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 5
	// string "UUID"
	o = append(o, 0x85, 0xa4, 0x55, 0x55, 0x49, 0x44)
	o = msgp.AppendBytes(o, z.UUID)
	// string "MajorVersion"
	o = append(o, 0xac, 0x4d, 0x61, 0x6a, 0x6f, 0x72, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e)
	o = msgp.AppendUint64(o, z.MajorVersion)
	// string "MicroVersion"
	o = append(o, 0xac, 0x4d, 0x69, 0x63, 0x72, 0x6f, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e)
	o = msgp.AppendUint32(o, z.MicroVersion)
	// string "Times"
	o = append(o, 0xa5, 0x54, 0x69, 0x6d, 0x65, 0x73)
	o = msgp.AppendArrayHeader(o, uint32(len(z.Times)))
	for zxvk := range z.Times {
		o = msgp.AppendInt64(o, z.Times[zxvk])
	}
	// string "Values"
	o = append(o, 0xa6, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x73)
	o = msgp.AppendArrayHeader(o, uint32(len(z.Values)))
	for zbzg := range z.Values {
		o = msgp.AppendFloat64(o, z.Values[zbzg])
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *JournalRecord) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zwht uint32
	zwht, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		return
	}
	for zwht > 0 {
		zwht--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "UUID":
			z.UUID, bts, err = msgp.ReadBytesBytes(bts, z.UUID)
			if err != nil {
				return
			}
		case "MajorVersion":
			z.MajorVersion, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				return
			}
		case "MicroVersion":
			z.MicroVersion, bts, err = msgp.ReadUint32Bytes(bts)
			if err != nil {
				return
			}
		case "Times":
			var zhct uint32
			zhct, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				return
			}
			if cap(z.Times) >= int(zhct) {
				z.Times = (z.Times)[:zhct]
			} else {
				z.Times = make([]int64, zhct)
			}
			for zxvk := range z.Times {
				z.Times[zxvk], bts, err = msgp.ReadInt64Bytes(bts)
				if err != nil {
					return
				}
			}
		case "Values":
			var zcua uint32
			zcua, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				return
			}
			if cap(z.Values) >= int(zcua) {
				z.Values = (z.Values)[:zcua]
			} else {
				z.Values = make([]float64, zcua)
			}
			for zbzg := range z.Values {
				z.Values[zbzg], bts, err = msgp.ReadFloat64Bytes(bts)
				if err != nil {
					return
				}
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				return
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *JournalRecord) Msgsize() (s int) {
	s = 1 + 5 + msgp.BytesPrefixSize + len(z.UUID) + 13 + msgp.Uint64Size + 13 + msgp.Uint32Size + 6 + msgp.ArrayHeaderSize + (len(z.Times) * (msgp.Int64Size)) + 7 + msgp.ArrayHeaderSize + (len(z.Values) * (msgp.Float64Size))
	return
}
