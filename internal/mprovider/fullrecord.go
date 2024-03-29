// Copyright (c) 2021 Michael Andersen
// Copyright (c) 2021 Regents of the University Of California
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

package mprovider

//go:generate msgp

type FullRecord struct {
	Collection string            `msg:"c"`
	Tags       map[string]string `msg:"t"`
	Anns       map[string]string `msg:"a"`
}

func (fr *FullRecord) setAnnotation(key string, value string) {
	fr.Anns[key] = value
}
func (fr *FullRecord) deleteAnnotation(key string) {
	delete(fr.Anns, key)
}
func (fr *FullRecord) Serialize() []byte {
	rv, err := fr.MarshalMsg(nil)
	if err != nil {
		panic(err)
	}
	return rv
}

func (em *etcdMetadataProvider) decodeFullRecord(r []byte) *FullRecord {
	fr := FullRecord{}
	fr.UnmarshalMsg(r)
	if fr.Tags == nil {
		fr.Tags = make(map[string]string)
	}
	if fr.Anns == nil {
		fr.Anns = make(map[string]string)
	}
	return &fr
}
