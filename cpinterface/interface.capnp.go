package cpinterface

// AUTO GENERATED - DO NOT EDIT

import (
	C "github.com/glycerine/go-capnproto"
	"math"
	"unsafe"
)

type Request C.Struct
type Request_Which uint16

const (
	REQUEST_VOID                   Request_Which = 0
	REQUEST_QUERYSTANDARDVALUES                  = 1
	REQUEST_QUERYSTATISTICALVALUES               = 2
	REQUEST_QUERYVERSION                         = 3
	REQUEST_QUERYNEARESTVALUE                    = 4
	REQUEST_QUERYCHANGEDRANGES                   = 5
	REQUEST_INSERTVALUES                         = 6
	REQUEST_DELETEVALUES                         = 7
)

func NewRequest(s *C.Segment) Request      { return Request(s.NewStruct(16, 1)) }
func NewRootRequest(s *C.Segment) Request  { return Request(s.NewRootStruct(16, 1)) }
func ReadRootRequest(s *C.Segment) Request { return Request(s.Root(0).ToStruct()) }
func (s Request) Which() Request_Which     { return Request_Which(C.Struct(s).Get16(8)) }
func (s Request) EchoTag() uint64          { return C.Struct(s).Get64(0) }
func (s Request) SetEchoTag(v uint64)      { C.Struct(s).Set64(0, v) }
func (s Request) QueryStandardValues() CmdQueryStandardValues {
	return CmdQueryStandardValues(C.Struct(s).GetObject(0).ToStruct())
}
func (s Request) SetQueryStandardValues(v CmdQueryStandardValues) {
	C.Struct(s).Set16(8, 1)
	C.Struct(s).SetObject(0, C.Object(v))
}
func (s Request) QueryStatisticalValues() CmdQueryStatisticalValues {
	return CmdQueryStatisticalValues(C.Struct(s).GetObject(0).ToStruct())
}
func (s Request) SetQueryStatisticalValues(v CmdQueryStatisticalValues) {
	C.Struct(s).Set16(8, 2)
	C.Struct(s).SetObject(0, C.Object(v))
}
func (s Request) QueryVersion() CmdQueryVersion {
	return CmdQueryVersion(C.Struct(s).GetObject(0).ToStruct())
}
func (s Request) SetQueryVersion(v CmdQueryVersion) {
	C.Struct(s).Set16(8, 3)
	C.Struct(s).SetObject(0, C.Object(v))
}
func (s Request) QueryNearestValue() CmdQueryNearestValue {
	return CmdQueryNearestValue(C.Struct(s).GetObject(0).ToStruct())
}
func (s Request) SetQueryNearestValue(v CmdQueryNearestValue) {
	C.Struct(s).Set16(8, 4)
	C.Struct(s).SetObject(0, C.Object(v))
}
func (s Request) QueryChangedRanges() CmdQueryChangedRanges {
	return CmdQueryChangedRanges(C.Struct(s).GetObject(0).ToStruct())
}
func (s Request) SetQueryChangedRanges(v CmdQueryChangedRanges) {
	C.Struct(s).Set16(8, 5)
	C.Struct(s).SetObject(0, C.Object(v))
}
func (s Request) InsertValues() CmdInsertValues {
	return CmdInsertValues(C.Struct(s).GetObject(0).ToStruct())
}
func (s Request) SetInsertValues(v CmdInsertValues) {
	C.Struct(s).Set16(8, 6)
	C.Struct(s).SetObject(0, C.Object(v))
}
func (s Request) DeleteValues() CmdDeleteValues {
	return CmdDeleteValues(C.Struct(s).GetObject(0).ToStruct())
}
func (s Request) SetDeleteValues(v CmdDeleteValues) {
	C.Struct(s).Set16(8, 7)
	C.Struct(s).SetObject(0, C.Object(v))
}

// capn.JSON_enabled == false so we stub MarshallJSON().
func (s *Request) MarshalJSON() (bs []byte, err error) {
	return
}

type Request_List C.PointerList

func NewRequestList(s *C.Segment, sz int) Request_List {
	return Request_List(s.NewCompositeList(16, 1, sz))
}
func (s Request_List) Len() int         { return C.PointerList(s).Len() }
func (s Request_List) At(i int) Request { return Request(C.PointerList(s).At(i).ToStruct()) }
func (s Request_List) ToArray() []Request {
	return *(*[]Request)(unsafe.Pointer(C.PointerList(s).ToArray()))
}

type Record C.Struct

func NewRecord(s *C.Segment) Record      { return Record(s.NewStruct(16, 0)) }
func NewRootRecord(s *C.Segment) Record  { return Record(s.NewRootStruct(16, 0)) }
func ReadRootRecord(s *C.Segment) Record { return Record(s.Root(0).ToStruct()) }
func (s Record) Time() int64             { return int64(C.Struct(s).Get64(0)) }
func (s Record) SetTime(v int64)         { C.Struct(s).Set64(0, uint64(v)) }
func (s Record) Value() float64          { return math.Float64frombits(C.Struct(s).Get64(8)) }
func (s Record) SetValue(v float64)      { C.Struct(s).Set64(8, math.Float64bits(v)) }

// capn.JSON_enabled == false so we stub MarshallJSON().
func (s *Record) MarshalJSON() (bs []byte, err error) {
	return
}

type Record_List C.PointerList

func NewRecordList(s *C.Segment, sz int) Record_List {
	return Record_List(s.NewCompositeList(16, 0, sz))
}
func (s Record_List) Len() int        { return C.PointerList(s).Len() }
func (s Record_List) At(i int) Record { return Record(C.PointerList(s).At(i).ToStruct()) }
func (s Record_List) ToArray() []Record {
	return *(*[]Record)(unsafe.Pointer(C.PointerList(s).ToArray()))
}

type StatisticalRecord C.Struct

func NewStatisticalRecord(s *C.Segment) StatisticalRecord {
	return StatisticalRecord(s.NewStruct(40, 0))
}
func NewRootStatisticalRecord(s *C.Segment) StatisticalRecord {
	return StatisticalRecord(s.NewRootStruct(40, 0))
}
func ReadRootStatisticalRecord(s *C.Segment) StatisticalRecord {
	return StatisticalRecord(s.Root(0).ToStruct())
}
func (s StatisticalRecord) Time() int64       { return int64(C.Struct(s).Get64(0)) }
func (s StatisticalRecord) SetTime(v int64)   { C.Struct(s).Set64(0, uint64(v)) }
func (s StatisticalRecord) Count() uint64     { return C.Struct(s).Get64(8) }
func (s StatisticalRecord) SetCount(v uint64) { C.Struct(s).Set64(8, v) }
func (s StatisticalRecord) Min() float64      { return math.Float64frombits(C.Struct(s).Get64(16)) }
func (s StatisticalRecord) SetMin(v float64)  { C.Struct(s).Set64(16, math.Float64bits(v)) }
func (s StatisticalRecord) Mean() float64     { return math.Float64frombits(C.Struct(s).Get64(24)) }
func (s StatisticalRecord) SetMean(v float64) { C.Struct(s).Set64(24, math.Float64bits(v)) }
func (s StatisticalRecord) Max() float64      { return math.Float64frombits(C.Struct(s).Get64(32)) }
func (s StatisticalRecord) SetMax(v float64)  { C.Struct(s).Set64(32, math.Float64bits(v)) }

// capn.JSON_enabled == false so we stub MarshallJSON().
func (s *StatisticalRecord) MarshalJSON() (bs []byte, err error) {
	return
}

type StatisticalRecord_List C.PointerList

func NewStatisticalRecordList(s *C.Segment, sz int) StatisticalRecord_List {
	return StatisticalRecord_List(s.NewCompositeList(40, 0, sz))
}
func (s StatisticalRecord_List) Len() int { return C.PointerList(s).Len() }
func (s StatisticalRecord_List) At(i int) StatisticalRecord {
	return StatisticalRecord(C.PointerList(s).At(i).ToStruct())
}
func (s StatisticalRecord_List) ToArray() []StatisticalRecord {
	return *(*[]StatisticalRecord)(unsafe.Pointer(C.PointerList(s).ToArray()))
}

type CmdQueryStandardValues C.Struct

func NewCmdQueryStandardValues(s *C.Segment) CmdQueryStandardValues {
	return CmdQueryStandardValues(s.NewStruct(24, 1))
}
func NewRootCmdQueryStandardValues(s *C.Segment) CmdQueryStandardValues {
	return CmdQueryStandardValues(s.NewRootStruct(24, 1))
}
func ReadRootCmdQueryStandardValues(s *C.Segment) CmdQueryStandardValues {
	return CmdQueryStandardValues(s.Root(0).ToStruct())
}
func (s CmdQueryStandardValues) Uuid() []byte         { return C.Struct(s).GetObject(0).ToData() }
func (s CmdQueryStandardValues) SetUuid(v []byte)     { C.Struct(s).SetObject(0, s.Segment.NewData(v)) }
func (s CmdQueryStandardValues) Version() uint64      { return C.Struct(s).Get64(0) }
func (s CmdQueryStandardValues) SetVersion(v uint64)  { C.Struct(s).Set64(0, v) }
func (s CmdQueryStandardValues) StartTime() int64     { return int64(C.Struct(s).Get64(8)) }
func (s CmdQueryStandardValues) SetStartTime(v int64) { C.Struct(s).Set64(8, uint64(v)) }
func (s CmdQueryStandardValues) EndTime() int64       { return int64(C.Struct(s).Get64(16)) }
func (s CmdQueryStandardValues) SetEndTime(v int64)   { C.Struct(s).Set64(16, uint64(v)) }

// capn.JSON_enabled == false so we stub MarshallJSON().
func (s *CmdQueryStandardValues) MarshalJSON() (bs []byte, err error) {
	return
}

type CmdQueryStandardValues_List C.PointerList

func NewCmdQueryStandardValuesList(s *C.Segment, sz int) CmdQueryStandardValues_List {
	return CmdQueryStandardValues_List(s.NewCompositeList(24, 1, sz))
}
func (s CmdQueryStandardValues_List) Len() int { return C.PointerList(s).Len() }
func (s CmdQueryStandardValues_List) At(i int) CmdQueryStandardValues {
	return CmdQueryStandardValues(C.PointerList(s).At(i).ToStruct())
}
func (s CmdQueryStandardValues_List) ToArray() []CmdQueryStandardValues {
	return *(*[]CmdQueryStandardValues)(unsafe.Pointer(C.PointerList(s).ToArray()))
}

type CmdQueryStatisticalValues C.Struct

func NewCmdQueryStatisticalValues(s *C.Segment) CmdQueryStatisticalValues {
	return CmdQueryStatisticalValues(s.NewStruct(32, 1))
}
func NewRootCmdQueryStatisticalValues(s *C.Segment) CmdQueryStatisticalValues {
	return CmdQueryStatisticalValues(s.NewRootStruct(32, 1))
}
func ReadRootCmdQueryStatisticalValues(s *C.Segment) CmdQueryStatisticalValues {
	return CmdQueryStatisticalValues(s.Root(0).ToStruct())
}
func (s CmdQueryStatisticalValues) Uuid() []byte          { return C.Struct(s).GetObject(0).ToData() }
func (s CmdQueryStatisticalValues) SetUuid(v []byte)      { C.Struct(s).SetObject(0, s.Segment.NewData(v)) }
func (s CmdQueryStatisticalValues) Version() uint64       { return C.Struct(s).Get64(0) }
func (s CmdQueryStatisticalValues) SetVersion(v uint64)   { C.Struct(s).Set64(0, v) }
func (s CmdQueryStatisticalValues) StartTime() int64      { return int64(C.Struct(s).Get64(8)) }
func (s CmdQueryStatisticalValues) SetStartTime(v int64)  { C.Struct(s).Set64(8, uint64(v)) }
func (s CmdQueryStatisticalValues) EndTime() int64        { return int64(C.Struct(s).Get64(16)) }
func (s CmdQueryStatisticalValues) SetEndTime(v int64)    { C.Struct(s).Set64(16, uint64(v)) }
func (s CmdQueryStatisticalValues) PointWidth() uint8     { return C.Struct(s).Get8(24) }
func (s CmdQueryStatisticalValues) SetPointWidth(v uint8) { C.Struct(s).Set8(24, v) }

// capn.JSON_enabled == false so we stub MarshallJSON().
func (s *CmdQueryStatisticalValues) MarshalJSON() (bs []byte, err error) {
	return
}

type CmdQueryStatisticalValues_List C.PointerList

func NewCmdQueryStatisticalValuesList(s *C.Segment, sz int) CmdQueryStatisticalValues_List {
	return CmdQueryStatisticalValues_List(s.NewCompositeList(32, 1, sz))
}
func (s CmdQueryStatisticalValues_List) Len() int { return C.PointerList(s).Len() }
func (s CmdQueryStatisticalValues_List) At(i int) CmdQueryStatisticalValues {
	return CmdQueryStatisticalValues(C.PointerList(s).At(i).ToStruct())
}
func (s CmdQueryStatisticalValues_List) ToArray() []CmdQueryStatisticalValues {
	return *(*[]CmdQueryStatisticalValues)(unsafe.Pointer(C.PointerList(s).ToArray()))
}

type CmdQueryVersion C.Struct

func NewCmdQueryVersion(s *C.Segment) CmdQueryVersion { return CmdQueryVersion(s.NewStruct(0, 1)) }
func NewRootCmdQueryVersion(s *C.Segment) CmdQueryVersion {
	return CmdQueryVersion(s.NewRootStruct(0, 1))
}
func ReadRootCmdQueryVersion(s *C.Segment) CmdQueryVersion {
	return CmdQueryVersion(s.Root(0).ToStruct())
}
func (s CmdQueryVersion) Uuids() C.DataList     { return C.DataList(C.Struct(s).GetObject(0)) }
func (s CmdQueryVersion) SetUuids(v C.DataList) { C.Struct(s).SetObject(0, C.Object(v)) }

// capn.JSON_enabled == false so we stub MarshallJSON().
func (s *CmdQueryVersion) MarshalJSON() (bs []byte, err error) {
	return
}

type CmdQueryVersion_List C.PointerList

func NewCmdQueryVersionList(s *C.Segment, sz int) CmdQueryVersion_List {
	return CmdQueryVersion_List(s.NewCompositeList(0, 1, sz))
}
func (s CmdQueryVersion_List) Len() int { return C.PointerList(s).Len() }
func (s CmdQueryVersion_List) At(i int) CmdQueryVersion {
	return CmdQueryVersion(C.PointerList(s).At(i).ToStruct())
}
func (s CmdQueryVersion_List) ToArray() []CmdQueryVersion {
	return *(*[]CmdQueryVersion)(unsafe.Pointer(C.PointerList(s).ToArray()))
}

type CmdQueryNearestValue C.Struct

func NewCmdQueryNearestValue(s *C.Segment) CmdQueryNearestValue {
	return CmdQueryNearestValue(s.NewStruct(24, 1))
}
func NewRootCmdQueryNearestValue(s *C.Segment) CmdQueryNearestValue {
	return CmdQueryNearestValue(s.NewRootStruct(24, 1))
}
func ReadRootCmdQueryNearestValue(s *C.Segment) CmdQueryNearestValue {
	return CmdQueryNearestValue(s.Root(0).ToStruct())
}
func (s CmdQueryNearestValue) Uuid() []byte        { return C.Struct(s).GetObject(0).ToData() }
func (s CmdQueryNearestValue) SetUuid(v []byte)    { C.Struct(s).SetObject(0, s.Segment.NewData(v)) }
func (s CmdQueryNearestValue) Version() uint64     { return C.Struct(s).Get64(0) }
func (s CmdQueryNearestValue) SetVersion(v uint64) { C.Struct(s).Set64(0, v) }
func (s CmdQueryNearestValue) Time() int64         { return int64(C.Struct(s).Get64(8)) }
func (s CmdQueryNearestValue) SetTime(v int64)     { C.Struct(s).Set64(8, uint64(v)) }
func (s CmdQueryNearestValue) Backward() bool      { return C.Struct(s).Get1(128) }
func (s CmdQueryNearestValue) SetBackward(v bool)  { C.Struct(s).Set1(128, v) }

// capn.JSON_enabled == false so we stub MarshallJSON().
func (s *CmdQueryNearestValue) MarshalJSON() (bs []byte, err error) {
	return
}

type CmdQueryNearestValue_List C.PointerList

func NewCmdQueryNearestValueList(s *C.Segment, sz int) CmdQueryNearestValue_List {
	return CmdQueryNearestValue_List(s.NewCompositeList(24, 1, sz))
}
func (s CmdQueryNearestValue_List) Len() int { return C.PointerList(s).Len() }
func (s CmdQueryNearestValue_List) At(i int) CmdQueryNearestValue {
	return CmdQueryNearestValue(C.PointerList(s).At(i).ToStruct())
}
func (s CmdQueryNearestValue_List) ToArray() []CmdQueryNearestValue {
	return *(*[]CmdQueryNearestValue)(unsafe.Pointer(C.PointerList(s).ToArray()))
}

type CmdQueryChangedRanges C.Struct

func NewCmdQueryChangedRanges(s *C.Segment) CmdQueryChangedRanges {
	return CmdQueryChangedRanges(s.NewStruct(24, 1))
}
func NewRootCmdQueryChangedRanges(s *C.Segment) CmdQueryChangedRanges {
	return CmdQueryChangedRanges(s.NewRootStruct(24, 1))
}
func ReadRootCmdQueryChangedRanges(s *C.Segment) CmdQueryChangedRanges {
	return CmdQueryChangedRanges(s.Root(0).ToStruct())
}
func (s CmdQueryChangedRanges) Uuid() []byte               { return C.Struct(s).GetObject(0).ToData() }
func (s CmdQueryChangedRanges) SetUuid(v []byte)           { C.Struct(s).SetObject(0, s.Segment.NewData(v)) }
func (s CmdQueryChangedRanges) FromGeneration() uint64     { return C.Struct(s).Get64(0) }
func (s CmdQueryChangedRanges) SetFromGeneration(v uint64) { C.Struct(s).Set64(0, v) }
func (s CmdQueryChangedRanges) ToGeneration() uint64       { return C.Struct(s).Get64(8) }
func (s CmdQueryChangedRanges) SetToGeneration(v uint64)   { C.Struct(s).Set64(8, v) }
func (s CmdQueryChangedRanges) Threshold() uint64          { return C.Struct(s).Get64(16) }
func (s CmdQueryChangedRanges) SetThreshold(v uint64)      { C.Struct(s).Set64(16, v) }

// capn.JSON_enabled == false so we stub MarshallJSON().
func (s *CmdQueryChangedRanges) MarshalJSON() (bs []byte, err error) {
	return
}

type CmdQueryChangedRanges_List C.PointerList

func NewCmdQueryChangedRangesList(s *C.Segment, sz int) CmdQueryChangedRanges_List {
	return CmdQueryChangedRanges_List(s.NewCompositeList(24, 1, sz))
}
func (s CmdQueryChangedRanges_List) Len() int { return C.PointerList(s).Len() }
func (s CmdQueryChangedRanges_List) At(i int) CmdQueryChangedRanges {
	return CmdQueryChangedRanges(C.PointerList(s).At(i).ToStruct())
}
func (s CmdQueryChangedRanges_List) ToArray() []CmdQueryChangedRanges {
	return *(*[]CmdQueryChangedRanges)(unsafe.Pointer(C.PointerList(s).ToArray()))
}

type CmdInsertValues C.Struct

func NewCmdInsertValues(s *C.Segment) CmdInsertValues { return CmdInsertValues(s.NewStruct(8, 2)) }
func NewRootCmdInsertValues(s *C.Segment) CmdInsertValues {
	return CmdInsertValues(s.NewRootStruct(8, 2))
}
func ReadRootCmdInsertValues(s *C.Segment) CmdInsertValues {
	return CmdInsertValues(s.Root(0).ToStruct())
}
func (s CmdInsertValues) Uuid() []byte            { return C.Struct(s).GetObject(0).ToData() }
func (s CmdInsertValues) SetUuid(v []byte)        { C.Struct(s).SetObject(0, s.Segment.NewData(v)) }
func (s CmdInsertValues) Values() Record_List     { return Record_List(C.Struct(s).GetObject(1)) }
func (s CmdInsertValues) SetValues(v Record_List) { C.Struct(s).SetObject(1, C.Object(v)) }
func (s CmdInsertValues) Sync() bool              { return C.Struct(s).Get1(0) }
func (s CmdInsertValues) SetSync(v bool)          { C.Struct(s).Set1(0, v) }

// capn.JSON_enabled == false so we stub MarshallJSON().
func (s *CmdInsertValues) MarshalJSON() (bs []byte, err error) {
	return
}

type CmdInsertValues_List C.PointerList

func NewCmdInsertValuesList(s *C.Segment, sz int) CmdInsertValues_List {
	return CmdInsertValues_List(s.NewCompositeList(8, 2, sz))
}
func (s CmdInsertValues_List) Len() int { return C.PointerList(s).Len() }
func (s CmdInsertValues_List) At(i int) CmdInsertValues {
	return CmdInsertValues(C.PointerList(s).At(i).ToStruct())
}
func (s CmdInsertValues_List) ToArray() []CmdInsertValues {
	return *(*[]CmdInsertValues)(unsafe.Pointer(C.PointerList(s).ToArray()))
}

type CmdDeleteValues C.Struct

func NewCmdDeleteValues(s *C.Segment) CmdDeleteValues { return CmdDeleteValues(s.NewStruct(16, 1)) }
func NewRootCmdDeleteValues(s *C.Segment) CmdDeleteValues {
	return CmdDeleteValues(s.NewRootStruct(16, 1))
}
func ReadRootCmdDeleteValues(s *C.Segment) CmdDeleteValues {
	return CmdDeleteValues(s.Root(0).ToStruct())
}
func (s CmdDeleteValues) Uuid() []byte         { return C.Struct(s).GetObject(0).ToData() }
func (s CmdDeleteValues) SetUuid(v []byte)     { C.Struct(s).SetObject(0, s.Segment.NewData(v)) }
func (s CmdDeleteValues) StartTime() int64     { return int64(C.Struct(s).Get64(0)) }
func (s CmdDeleteValues) SetStartTime(v int64) { C.Struct(s).Set64(0, uint64(v)) }
func (s CmdDeleteValues) EndTime() int64       { return int64(C.Struct(s).Get64(8)) }
func (s CmdDeleteValues) SetEndTime(v int64)   { C.Struct(s).Set64(8, uint64(v)) }

// capn.JSON_enabled == false so we stub MarshallJSON().
func (s *CmdDeleteValues) MarshalJSON() (bs []byte, err error) {
	return
}

type CmdDeleteValues_List C.PointerList

func NewCmdDeleteValuesList(s *C.Segment, sz int) CmdDeleteValues_List {
	return CmdDeleteValues_List(s.NewCompositeList(16, 1, sz))
}
func (s CmdDeleteValues_List) Len() int { return C.PointerList(s).Len() }
func (s CmdDeleteValues_List) At(i int) CmdDeleteValues {
	return CmdDeleteValues(C.PointerList(s).At(i).ToStruct())
}
func (s CmdDeleteValues_List) ToArray() []CmdDeleteValues {
	return *(*[]CmdDeleteValues)(unsafe.Pointer(C.PointerList(s).ToArray()))
}

type Response C.Struct
type Response_Which uint16

const (
	RESPONSE_VOID               Response_Which = 0
	RESPONSE_RECORDS                           = 1
	RESPONSE_STATISTICALRECORDS                = 2
	RESPONSE_VERSIONLIST                       = 3
	RESPONSE_CHANGEDRNGLIST                    = 4
)

func NewResponse(s *C.Segment) Response       { return Response(s.NewStruct(16, 1)) }
func NewRootResponse(s *C.Segment) Response   { return Response(s.NewRootStruct(16, 1)) }
func ReadRootResponse(s *C.Segment) Response  { return Response(s.Root(0).ToStruct()) }
func (s Response) Which() Response_Which      { return Response_Which(C.Struct(s).Get16(10)) }
func (s Response) EchoTag() uint64            { return C.Struct(s).Get64(0) }
func (s Response) SetEchoTag(v uint64)        { C.Struct(s).Set64(0, v) }
func (s Response) StatusCode() StatusCode     { return StatusCode(C.Struct(s).Get16(8)) }
func (s Response) SetStatusCode(v StatusCode) { C.Struct(s).Set16(8, uint16(v)) }
func (s Response) Records() Records           { return Records(C.Struct(s).GetObject(0).ToStruct()) }
func (s Response) SetRecords(v Records) {
	C.Struct(s).Set16(10, 1)
	C.Struct(s).SetObject(0, C.Object(v))
}
func (s Response) StatisticalRecords() StatisticalRecords {
	return StatisticalRecords(C.Struct(s).GetObject(0).ToStruct())
}
func (s Response) SetStatisticalRecords(v StatisticalRecords) {
	C.Struct(s).Set16(10, 2)
	C.Struct(s).SetObject(0, C.Object(v))
}
func (s Response) VersionList() Versions { return Versions(C.Struct(s).GetObject(0).ToStruct()) }
func (s Response) SetVersionList(v Versions) {
	C.Struct(s).Set16(10, 3)
	C.Struct(s).SetObject(0, C.Object(v))
}
func (s Response) ChangedRngList() Ranges { return Ranges(C.Struct(s).GetObject(0).ToStruct()) }
func (s Response) SetChangedRngList(v Ranges) {
	C.Struct(s).Set16(10, 4)
	C.Struct(s).SetObject(0, C.Object(v))
}

// capn.JSON_enabled == false so we stub MarshallJSON().
func (s *Response) MarshalJSON() (bs []byte, err error) {
	return
}

type Response_List C.PointerList

func NewResponseList(s *C.Segment, sz int) Response_List {
	return Response_List(s.NewCompositeList(16, 1, sz))
}
func (s Response_List) Len() int          { return C.PointerList(s).Len() }
func (s Response_List) At(i int) Response { return Response(C.PointerList(s).At(i).ToStruct()) }
func (s Response_List) ToArray() []Response {
	return *(*[]Response)(unsafe.Pointer(C.PointerList(s).ToArray()))
}

type StatusCode uint16

const (
	STATUSCODE_OK                    StatusCode = 0
	STATUSCODE_INTERNALERROR         StatusCode = 1
	STATUSCODE_NOSUCHSTREAMORVERSION StatusCode = 2
	STATUSCODE_INVALIDPARAMETER      StatusCode = 3
	STATUSCODE_NOSUCHPOINT           StatusCode = 4
)

func (c StatusCode) String() string {
	switch c {
	case STATUSCODE_OK:
		return "ok"
	case STATUSCODE_INTERNALERROR:
		return "internalError"
	case STATUSCODE_NOSUCHSTREAMORVERSION:
		return "noSuchStreamOrVersion"
	case STATUSCODE_INVALIDPARAMETER:
		return "invalidParameter"
	case STATUSCODE_NOSUCHPOINT:
		return "noSuchPoint"
	default:
		return ""
	}
}

type StatusCode_List C.PointerList

func NewStatusCodeList(s *C.Segment, sz int) StatusCode_List {
	return StatusCode_List(s.NewUInt16List(sz))
}
func (s StatusCode_List) Len() int            { return C.UInt16List(s).Len() }
func (s StatusCode_List) At(i int) StatusCode { return StatusCode(C.UInt16List(s).At(i)) }
func (s StatusCode_List) ToArray() []StatusCode {
	return *(*[]StatusCode)(unsafe.Pointer(C.UInt16List(s).ToEnumArray()))
}

type Records C.Struct

func NewRecords(s *C.Segment) Records      { return Records(s.NewStruct(8, 1)) }
func NewRootRecords(s *C.Segment) Records  { return Records(s.NewRootStruct(8, 1)) }
func ReadRootRecords(s *C.Segment) Records { return Records(s.Root(0).ToStruct()) }
func (s Records) Version() uint64          { return C.Struct(s).Get64(0) }
func (s Records) SetVersion(v uint64)      { C.Struct(s).Set64(0, v) }
func (s Records) Values() Record_List      { return Record_List(C.Struct(s).GetObject(0)) }
func (s Records) SetValues(v Record_List)  { C.Struct(s).SetObject(0, C.Object(v)) }

// capn.JSON_enabled == false so we stub MarshallJSON().
func (s *Records) MarshalJSON() (bs []byte, err error) {
	return
}

type Records_List C.PointerList

func NewRecordsList(s *C.Segment, sz int) Records_List {
	return Records_List(s.NewCompositeList(8, 1, sz))
}
func (s Records_List) Len() int         { return C.PointerList(s).Len() }
func (s Records_List) At(i int) Records { return Records(C.PointerList(s).At(i).ToStruct()) }
func (s Records_List) ToArray() []Records {
	return *(*[]Records)(unsafe.Pointer(C.PointerList(s).ToArray()))
}

type StatisticalRecords C.Struct

func NewStatisticalRecords(s *C.Segment) StatisticalRecords {
	return StatisticalRecords(s.NewStruct(8, 1))
}
func NewRootStatisticalRecords(s *C.Segment) StatisticalRecords {
	return StatisticalRecords(s.NewRootStruct(8, 1))
}
func ReadRootStatisticalRecords(s *C.Segment) StatisticalRecords {
	return StatisticalRecords(s.Root(0).ToStruct())
}
func (s StatisticalRecords) Version() uint64     { return C.Struct(s).Get64(0) }
func (s StatisticalRecords) SetVersion(v uint64) { C.Struct(s).Set64(0, v) }
func (s StatisticalRecords) Values() StatisticalRecord_List {
	return StatisticalRecord_List(C.Struct(s).GetObject(0))
}
func (s StatisticalRecords) SetValues(v StatisticalRecord_List) { C.Struct(s).SetObject(0, C.Object(v)) }

// capn.JSON_enabled == false so we stub MarshallJSON().
func (s *StatisticalRecords) MarshalJSON() (bs []byte, err error) {
	return
}

type StatisticalRecords_List C.PointerList

func NewStatisticalRecordsList(s *C.Segment, sz int) StatisticalRecords_List {
	return StatisticalRecords_List(s.NewCompositeList(8, 1, sz))
}
func (s StatisticalRecords_List) Len() int { return C.PointerList(s).Len() }
func (s StatisticalRecords_List) At(i int) StatisticalRecords {
	return StatisticalRecords(C.PointerList(s).At(i).ToStruct())
}
func (s StatisticalRecords_List) ToArray() []StatisticalRecords {
	return *(*[]StatisticalRecords)(unsafe.Pointer(C.PointerList(s).ToArray()))
}

type Versions C.Struct

func NewVersions(s *C.Segment) Versions       { return Versions(s.NewStruct(0, 2)) }
func NewRootVersions(s *C.Segment) Versions   { return Versions(s.NewRootStruct(0, 2)) }
func ReadRootVersions(s *C.Segment) Versions  { return Versions(s.Root(0).ToStruct()) }
func (s Versions) Uuids() C.DataList          { return C.DataList(C.Struct(s).GetObject(0)) }
func (s Versions) SetUuids(v C.DataList)      { C.Struct(s).SetObject(0, C.Object(v)) }
func (s Versions) Versions() C.UInt64List     { return C.UInt64List(C.Struct(s).GetObject(1)) }
func (s Versions) SetVersions(v C.UInt64List) { C.Struct(s).SetObject(1, C.Object(v)) }

// capn.JSON_enabled == false so we stub MarshallJSON().
func (s *Versions) MarshalJSON() (bs []byte, err error) {
	return
}

type Versions_List C.PointerList

func NewVersionsList(s *C.Segment, sz int) Versions_List {
	return Versions_List(s.NewCompositeList(0, 2, sz))
}
func (s Versions_List) Len() int          { return C.PointerList(s).Len() }
func (s Versions_List) At(i int) Versions { return Versions(C.PointerList(s).At(i).ToStruct()) }
func (s Versions_List) ToArray() []Versions {
	return *(*[]Versions)(unsafe.Pointer(C.PointerList(s).ToArray()))
}

type ChangedRange C.Struct

func NewChangedRange(s *C.Segment) ChangedRange      { return ChangedRange(s.NewStruct(16, 0)) }
func NewRootChangedRange(s *C.Segment) ChangedRange  { return ChangedRange(s.NewRootStruct(16, 0)) }
func ReadRootChangedRange(s *C.Segment) ChangedRange { return ChangedRange(s.Root(0).ToStruct()) }
func (s ChangedRange) StartTime() int64              { return int64(C.Struct(s).Get64(0)) }
func (s ChangedRange) SetStartTime(v int64)          { C.Struct(s).Set64(0, uint64(v)) }
func (s ChangedRange) EndTime() int64                { return int64(C.Struct(s).Get64(8)) }
func (s ChangedRange) SetEndTime(v int64)            { C.Struct(s).Set64(8, uint64(v)) }

// capn.JSON_enabled == false so we stub MarshallJSON().
func (s *ChangedRange) MarshalJSON() (bs []byte, err error) {
	return
}

type ChangedRange_List C.PointerList

func NewChangedRangeList(s *C.Segment, sz int) ChangedRange_List {
	return ChangedRange_List(s.NewCompositeList(16, 0, sz))
}
func (s ChangedRange_List) Len() int { return C.PointerList(s).Len() }
func (s ChangedRange_List) At(i int) ChangedRange {
	return ChangedRange(C.PointerList(s).At(i).ToStruct())
}
func (s ChangedRange_List) ToArray() []ChangedRange {
	return *(*[]ChangedRange)(unsafe.Pointer(C.PointerList(s).ToArray()))
}

type Ranges C.Struct

func NewRanges(s *C.Segment) Ranges            { return Ranges(s.NewStruct(8, 1)) }
func NewRootRanges(s *C.Segment) Ranges        { return Ranges(s.NewRootStruct(8, 1)) }
func ReadRootRanges(s *C.Segment) Ranges       { return Ranges(s.Root(0).ToStruct()) }
func (s Ranges) Version() uint64               { return C.Struct(s).Get64(0) }
func (s Ranges) SetVersion(v uint64)           { C.Struct(s).Set64(0, v) }
func (s Ranges) Values() ChangedRange_List     { return ChangedRange_List(C.Struct(s).GetObject(0)) }
func (s Ranges) SetValues(v ChangedRange_List) { C.Struct(s).SetObject(0, C.Object(v)) }

// capn.JSON_enabled == false so we stub MarshallJSON().
func (s *Ranges) MarshalJSON() (bs []byte, err error) {
	return
}

type Ranges_List C.PointerList

func NewRangesList(s *C.Segment, sz int) Ranges_List { return Ranges_List(s.NewCompositeList(8, 1, sz)) }
func (s Ranges_List) Len() int                       { return C.PointerList(s).Len() }
func (s Ranges_List) At(i int) Ranges                { return Ranges(C.PointerList(s).At(i).ToStruct()) }
func (s Ranges_List) ToArray() []Ranges {
	return *(*[]Ranges)(unsafe.Pointer(C.PointerList(s).ToArray()))
}
