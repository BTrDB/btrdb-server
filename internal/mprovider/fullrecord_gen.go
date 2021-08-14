// Copyright (c) 2021 Michael Andersen
// Copyright (c) 2021 Regents of the University Of California
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

package mprovider

// NOTE: THIS FILE WAS PRODUCED BY THE
// MSGP CODE GENERATION TOOL (github.com/tinylib/msgp)
// DO NOT EDIT

import "github.com/tinylib/msgp/msgp"

// DecodeMsg implements msgp.Decodable
func (z *FullRecord) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zajw uint32
	zajw, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	for zajw > 0 {
		zajw--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "c":
			z.Collection, err = dc.ReadString()
			if err != nil {
				return
			}
		case "t":
			var zwht uint32
			zwht, err = dc.ReadMapHeader()
			if err != nil {
				return
			}
			if z.Tags == nil && zwht > 0 {
				z.Tags = make(map[string]string, zwht)
			} else if len(z.Tags) > 0 {
				for key, _ := range z.Tags {
					delete(z.Tags, key)
				}
			}
			for zwht > 0 {
				zwht--
				var zxvk string
				var zbzg string
				zxvk, err = dc.ReadString()
				if err != nil {
					return
				}
				zbzg, err = dc.ReadString()
				if err != nil {
					return
				}
				z.Tags[zxvk] = zbzg
			}
		case "a":
			var zhct uint32
			zhct, err = dc.ReadMapHeader()
			if err != nil {
				return
			}
			if z.Anns == nil && zhct > 0 {
				z.Anns = make(map[string]string, zhct)
			} else if len(z.Anns) > 0 {
				for key, _ := range z.Anns {
					delete(z.Anns, key)
				}
			}
			for zhct > 0 {
				zhct--
				var zbai string
				var zcmr string
				zbai, err = dc.ReadString()
				if err != nil {
					return
				}
				zcmr, err = dc.ReadString()
				if err != nil {
					return
				}
				z.Anns[zbai] = zcmr
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
func (z *FullRecord) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 3
	// write "c"
	err = en.Append(0x83, 0xa1, 0x63)
	if err != nil {
		return err
	}
	err = en.WriteString(z.Collection)
	if err != nil {
		return
	}
	// write "t"
	err = en.Append(0xa1, 0x74)
	if err != nil {
		return err
	}
	err = en.WriteMapHeader(uint32(len(z.Tags)))
	if err != nil {
		return
	}
	for zxvk, zbzg := range z.Tags {
		err = en.WriteString(zxvk)
		if err != nil {
			return
		}
		err = en.WriteString(zbzg)
		if err != nil {
			return
		}
	}
	// write "a"
	err = en.Append(0xa1, 0x61)
	if err != nil {
		return err
	}
	err = en.WriteMapHeader(uint32(len(z.Anns)))
	if err != nil {
		return
	}
	for zbai, zcmr := range z.Anns {
		err = en.WriteString(zbai)
		if err != nil {
			return
		}
		err = en.WriteString(zcmr)
		if err != nil {
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *FullRecord) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 3
	// string "c"
	o = append(o, 0x83, 0xa1, 0x63)
	o = msgp.AppendString(o, z.Collection)
	// string "t"
	o = append(o, 0xa1, 0x74)
	o = msgp.AppendMapHeader(o, uint32(len(z.Tags)))
	for zxvk, zbzg := range z.Tags {
		o = msgp.AppendString(o, zxvk)
		o = msgp.AppendString(o, zbzg)
	}
	// string "a"
	o = append(o, 0xa1, 0x61)
	o = msgp.AppendMapHeader(o, uint32(len(z.Anns)))
	for zbai, zcmr := range z.Anns {
		o = msgp.AppendString(o, zbai)
		o = msgp.AppendString(o, zcmr)
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *FullRecord) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zcua uint32
	zcua, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		return
	}
	for zcua > 0 {
		zcua--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "c":
			z.Collection, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				return
			}
		case "t":
			var zxhx uint32
			zxhx, bts, err = msgp.ReadMapHeaderBytes(bts)
			if err != nil {
				return
			}
			if z.Tags == nil && zxhx > 0 {
				z.Tags = make(map[string]string, zxhx)
			} else if len(z.Tags) > 0 {
				for key, _ := range z.Tags {
					delete(z.Tags, key)
				}
			}
			for zxhx > 0 {
				var zxvk string
				var zbzg string
				zxhx--
				zxvk, bts, err = msgp.ReadStringBytes(bts)
				if err != nil {
					return
				}
				zbzg, bts, err = msgp.ReadStringBytes(bts)
				if err != nil {
					return
				}
				z.Tags[zxvk] = zbzg
			}
		case "a":
			var zlqf uint32
			zlqf, bts, err = msgp.ReadMapHeaderBytes(bts)
			if err != nil {
				return
			}
			if z.Anns == nil && zlqf > 0 {
				z.Anns = make(map[string]string, zlqf)
			} else if len(z.Anns) > 0 {
				for key, _ := range z.Anns {
					delete(z.Anns, key)
				}
			}
			for zlqf > 0 {
				var zbai string
				var zcmr string
				zlqf--
				zbai, bts, err = msgp.ReadStringBytes(bts)
				if err != nil {
					return
				}
				zcmr, bts, err = msgp.ReadStringBytes(bts)
				if err != nil {
					return
				}
				z.Anns[zbai] = zcmr
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
func (z *FullRecord) Msgsize() (s int) {
	s = 1 + 2 + msgp.StringPrefixSize + len(z.Collection) + 2 + msgp.MapHeaderSize
	if z.Tags != nil {
		for zxvk, zbzg := range z.Tags {
			_ = zbzg
			s += msgp.StringPrefixSize + len(zxvk) + msgp.StringPrefixSize + len(zbzg)
		}
	}
	s += 2 + msgp.MapHeaderSize
	if z.Anns != nil {
		for zbai, zcmr := range z.Anns {
			_ = zcmr
			s += msgp.StringPrefixSize + len(zbai) + msgp.StringPrefixSize + len(zcmr)
		}
	}
	return
}
