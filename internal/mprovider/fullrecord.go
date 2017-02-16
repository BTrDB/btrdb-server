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
func (fr *FullRecord) serialize() []byte {
	rv, err := fr.MarshalMsg(nil)
	if err != nil {
		panic(err)
	}
	return rv
}

func (em *etcdMetadataProvider) decodeFullRecord(r []byte) *FullRecord {
	fr := FullRecord{}
	fr.UnmarshalMsg(r)
	return &fr
}
