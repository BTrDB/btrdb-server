package jprovider

//go:generate msgp -tests=false

type JournalRecord struct {
	//The stream UUID
	UUID []byte `msgpack:"u"`
	//The version of the stream that this would appear in
	MajorVersion uint64 `msgpack:"f"`
	//The microversion that this represents
	MicroVersion uint32 `msgpack:"m"`
	//Data point times
	Times []int64 `msgpack:"t"`
	//Data point valuez
	Values []float64 `msgpack:"v"`
}
