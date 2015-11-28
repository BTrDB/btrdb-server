package cpinterface

// AUTO GENERATED - DO NOT EDIT

import (
	"bufio"
	"bytes"
	"encoding/json"
	C "github.com/glycerine/go-capnproto"
	"io"
	"math"
)

type Request C.Struct
type Request_Which uint16

const (
	REQUEST_VOID                   Request_Which = 0
	REQUEST_QUERYSTANDARDVALUES    Request_Which = 1
	REQUEST_QUERYSTATISTICALVALUES Request_Which = 2
	REQUEST_QUERYWINDOWVALUES      Request_Which = 8
	REQUEST_QUERYVERSION           Request_Which = 3
	REQUEST_QUERYNEARESTVALUE      Request_Which = 4
	REQUEST_QUERYCHANGEDRANGES     Request_Which = 5
	REQUEST_INSERTVALUES           Request_Which = 6
	REQUEST_DELETEVALUES           Request_Which = 7
)

func NewRequest(s *C.Segment) Request      { return Request(s.NewStruct(16, 1)) }
func NewRootRequest(s *C.Segment) Request  { return Request(s.NewRootStruct(16, 1)) }
func AutoNewRequest(s *C.Segment) Request  { return Request(s.NewStructAR(16, 1)) }
func ReadRootRequest(s *C.Segment) Request { return Request(s.Root(0).ToStruct()) }
func (s Request) Which() Request_Which     { return Request_Which(C.Struct(s).Get16(8)) }
func (s Request) EchoTag() uint64          { return C.Struct(s).Get64(0) }
func (s Request) SetEchoTag(v uint64)      { C.Struct(s).Set64(0, v) }
func (s Request) SetVoid()                 { C.Struct(s).Set16(8, 0) }
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
func (s Request) QueryWindowValues() CmdQueryWindowValues {
	return CmdQueryWindowValues(C.Struct(s).GetObject(0).ToStruct())
}
func (s Request) SetQueryWindowValues(v CmdQueryWindowValues) {
	C.Struct(s).Set16(8, 8)
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
func (s Request) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"echoTag\":")
	if err != nil {
		return err
	}
	{
		s := s.EchoTag()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	if s.Which() == REQUEST_VOID {
		_, err = b.WriteString("\"void\":")
		if err != nil {
			return err
		}
		_ = s
		_, err = b.WriteString("null")
		if err != nil {
			return err
		}
	}
	if s.Which() == REQUEST_QUERYSTANDARDVALUES {
		_, err = b.WriteString("\"queryStandardValues\":")
		if err != nil {
			return err
		}
		{
			s := s.QueryStandardValues()
			err = s.WriteJSON(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == REQUEST_QUERYSTATISTICALVALUES {
		_, err = b.WriteString("\"queryStatisticalValues\":")
		if err != nil {
			return err
		}
		{
			s := s.QueryStatisticalValues()
			err = s.WriteJSON(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == REQUEST_QUERYWINDOWVALUES {
		_, err = b.WriteString("\"queryWindowValues\":")
		if err != nil {
			return err
		}
		{
			s := s.QueryWindowValues()
			err = s.WriteJSON(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == REQUEST_QUERYVERSION {
		_, err = b.WriteString("\"queryVersion\":")
		if err != nil {
			return err
		}
		{
			s := s.QueryVersion()
			err = s.WriteJSON(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == REQUEST_QUERYNEARESTVALUE {
		_, err = b.WriteString("\"queryNearestValue\":")
		if err != nil {
			return err
		}
		{
			s := s.QueryNearestValue()
			err = s.WriteJSON(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == REQUEST_QUERYCHANGEDRANGES {
		_, err = b.WriteString("\"queryChangedRanges\":")
		if err != nil {
			return err
		}
		{
			s := s.QueryChangedRanges()
			err = s.WriteJSON(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == REQUEST_INSERTVALUES {
		_, err = b.WriteString("\"insertValues\":")
		if err != nil {
			return err
		}
		{
			s := s.InsertValues()
			err = s.WriteJSON(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == REQUEST_DELETEVALUES {
		_, err = b.WriteString("\"deleteValues\":")
		if err != nil {
			return err
		}
		{
			s := s.DeleteValues()
			err = s.WriteJSON(b)
			if err != nil {
				return err
			}
		}
	}
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s Request) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s Request) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("echoTag = ")
	if err != nil {
		return err
	}
	{
		s := s.EchoTag()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	if s.Which() == REQUEST_VOID {
		_, err = b.WriteString("void = ")
		if err != nil {
			return err
		}
		_ = s
		_, err = b.WriteString("null")
		if err != nil {
			return err
		}
	}
	if s.Which() == REQUEST_QUERYSTANDARDVALUES {
		_, err = b.WriteString("queryStandardValues = ")
		if err != nil {
			return err
		}
		{
			s := s.QueryStandardValues()
			err = s.WriteCapLit(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == REQUEST_QUERYSTATISTICALVALUES {
		_, err = b.WriteString("queryStatisticalValues = ")
		if err != nil {
			return err
		}
		{
			s := s.QueryStatisticalValues()
			err = s.WriteCapLit(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == REQUEST_QUERYWINDOWVALUES {
		_, err = b.WriteString("queryWindowValues = ")
		if err != nil {
			return err
		}
		{
			s := s.QueryWindowValues()
			err = s.WriteCapLit(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == REQUEST_QUERYVERSION {
		_, err = b.WriteString("queryVersion = ")
		if err != nil {
			return err
		}
		{
			s := s.QueryVersion()
			err = s.WriteCapLit(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == REQUEST_QUERYNEARESTVALUE {
		_, err = b.WriteString("queryNearestValue = ")
		if err != nil {
			return err
		}
		{
			s := s.QueryNearestValue()
			err = s.WriteCapLit(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == REQUEST_QUERYCHANGEDRANGES {
		_, err = b.WriteString("queryChangedRanges = ")
		if err != nil {
			return err
		}
		{
			s := s.QueryChangedRanges()
			err = s.WriteCapLit(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == REQUEST_INSERTVALUES {
		_, err = b.WriteString("insertValues = ")
		if err != nil {
			return err
		}
		{
			s := s.InsertValues()
			err = s.WriteCapLit(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == REQUEST_DELETEVALUES {
		_, err = b.WriteString("deleteValues = ")
		if err != nil {
			return err
		}
		{
			s := s.DeleteValues()
			err = s.WriteCapLit(b)
			if err != nil {
				return err
			}
		}
	}
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s Request) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type Request_List C.PointerList

func NewRequestList(s *C.Segment, sz int) Request_List {
	return Request_List(s.NewCompositeList(16, 1, sz))
}
func (s Request_List) Len() int         { return C.PointerList(s).Len() }
func (s Request_List) At(i int) Request { return Request(C.PointerList(s).At(i).ToStruct()) }
func (s Request_List) ToArray() []Request {
	n := s.Len()
	a := make([]Request, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s Request_List) Set(i int, item Request) { C.PointerList(s).Set(i, C.Object(item)) }

type Record C.Struct

func NewRecord(s *C.Segment) Record      { return Record(s.NewStruct(16, 0)) }
func NewRootRecord(s *C.Segment) Record  { return Record(s.NewRootStruct(16, 0)) }
func AutoNewRecord(s *C.Segment) Record  { return Record(s.NewStructAR(16, 0)) }
func ReadRootRecord(s *C.Segment) Record { return Record(s.Root(0).ToStruct()) }
func (s Record) Time() int64             { return int64(C.Struct(s).Get64(0)) }
func (s Record) SetTime(v int64)         { C.Struct(s).Set64(0, uint64(v)) }
func (s Record) Value() float64          { return math.Float64frombits(C.Struct(s).Get64(8)) }
func (s Record) SetValue(v float64)      { C.Struct(s).Set64(8, math.Float64bits(v)) }
func (s Record) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"time\":")
	if err != nil {
		return err
	}
	{
		s := s.Time()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"value\":")
	if err != nil {
		return err
	}
	{
		s := s.Value()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s Record) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s Record) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("time = ")
	if err != nil {
		return err
	}
	{
		s := s.Time()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("value = ")
	if err != nil {
		return err
	}
	{
		s := s.Value()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s Record) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type Record_List C.PointerList

func NewRecordList(s *C.Segment, sz int) Record_List {
	return Record_List(s.NewCompositeList(16, 0, sz))
}
func (s Record_List) Len() int        { return C.PointerList(s).Len() }
func (s Record_List) At(i int) Record { return Record(C.PointerList(s).At(i).ToStruct()) }
func (s Record_List) ToArray() []Record {
	n := s.Len()
	a := make([]Record, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s Record_List) Set(i int, item Record) { C.PointerList(s).Set(i, C.Object(item)) }

type StatisticalRecord C.Struct

func NewStatisticalRecord(s *C.Segment) StatisticalRecord {
	return StatisticalRecord(s.NewStruct(40, 0))
}
func NewRootStatisticalRecord(s *C.Segment) StatisticalRecord {
	return StatisticalRecord(s.NewRootStruct(40, 0))
}
func AutoNewStatisticalRecord(s *C.Segment) StatisticalRecord {
	return StatisticalRecord(s.NewStructAR(40, 0))
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
func (s StatisticalRecord) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"time\":")
	if err != nil {
		return err
	}
	{
		s := s.Time()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"count\":")
	if err != nil {
		return err
	}
	{
		s := s.Count()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"min\":")
	if err != nil {
		return err
	}
	{
		s := s.Min()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"mean\":")
	if err != nil {
		return err
	}
	{
		s := s.Mean()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"max\":")
	if err != nil {
		return err
	}
	{
		s := s.Max()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s StatisticalRecord) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s StatisticalRecord) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("time = ")
	if err != nil {
		return err
	}
	{
		s := s.Time()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("count = ")
	if err != nil {
		return err
	}
	{
		s := s.Count()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("min = ")
	if err != nil {
		return err
	}
	{
		s := s.Min()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("mean = ")
	if err != nil {
		return err
	}
	{
		s := s.Mean()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("max = ")
	if err != nil {
		return err
	}
	{
		s := s.Max()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s StatisticalRecord) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
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
	n := s.Len()
	a := make([]StatisticalRecord, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s StatisticalRecord_List) Set(i int, item StatisticalRecord) {
	C.PointerList(s).Set(i, C.Object(item))
}

type CmdQueryStandardValues C.Struct

func NewCmdQueryStandardValues(s *C.Segment) CmdQueryStandardValues {
	return CmdQueryStandardValues(s.NewStruct(24, 1))
}
func NewRootCmdQueryStandardValues(s *C.Segment) CmdQueryStandardValues {
	return CmdQueryStandardValues(s.NewRootStruct(24, 1))
}
func AutoNewCmdQueryStandardValues(s *C.Segment) CmdQueryStandardValues {
	return CmdQueryStandardValues(s.NewStructAR(24, 1))
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
func (s CmdQueryStandardValues) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"uuid\":")
	if err != nil {
		return err
	}
	{
		s := s.Uuid()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"version\":")
	if err != nil {
		return err
	}
	{
		s := s.Version()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"startTime\":")
	if err != nil {
		return err
	}
	{
		s := s.StartTime()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"endTime\":")
	if err != nil {
		return err
	}
	{
		s := s.EndTime()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s CmdQueryStandardValues) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s CmdQueryStandardValues) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("uuid = ")
	if err != nil {
		return err
	}
	{
		s := s.Uuid()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("version = ")
	if err != nil {
		return err
	}
	{
		s := s.Version()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("startTime = ")
	if err != nil {
		return err
	}
	{
		s := s.StartTime()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("endTime = ")
	if err != nil {
		return err
	}
	{
		s := s.EndTime()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s CmdQueryStandardValues) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
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
	n := s.Len()
	a := make([]CmdQueryStandardValues, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s CmdQueryStandardValues_List) Set(i int, item CmdQueryStandardValues) {
	C.PointerList(s).Set(i, C.Object(item))
}

type CmdQueryStatisticalValues C.Struct

func NewCmdQueryStatisticalValues(s *C.Segment) CmdQueryStatisticalValues {
	return CmdQueryStatisticalValues(s.NewStruct(32, 1))
}
func NewRootCmdQueryStatisticalValues(s *C.Segment) CmdQueryStatisticalValues {
	return CmdQueryStatisticalValues(s.NewRootStruct(32, 1))
}
func AutoNewCmdQueryStatisticalValues(s *C.Segment) CmdQueryStatisticalValues {
	return CmdQueryStatisticalValues(s.NewStructAR(32, 1))
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
func (s CmdQueryStatisticalValues) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"uuid\":")
	if err != nil {
		return err
	}
	{
		s := s.Uuid()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"version\":")
	if err != nil {
		return err
	}
	{
		s := s.Version()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"startTime\":")
	if err != nil {
		return err
	}
	{
		s := s.StartTime()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"endTime\":")
	if err != nil {
		return err
	}
	{
		s := s.EndTime()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"pointWidth\":")
	if err != nil {
		return err
	}
	{
		s := s.PointWidth()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s CmdQueryStatisticalValues) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s CmdQueryStatisticalValues) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("uuid = ")
	if err != nil {
		return err
	}
	{
		s := s.Uuid()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("version = ")
	if err != nil {
		return err
	}
	{
		s := s.Version()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("startTime = ")
	if err != nil {
		return err
	}
	{
		s := s.StartTime()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("endTime = ")
	if err != nil {
		return err
	}
	{
		s := s.EndTime()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("pointWidth = ")
	if err != nil {
		return err
	}
	{
		s := s.PointWidth()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s CmdQueryStatisticalValues) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
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
	n := s.Len()
	a := make([]CmdQueryStatisticalValues, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s CmdQueryStatisticalValues_List) Set(i int, item CmdQueryStatisticalValues) {
	C.PointerList(s).Set(i, C.Object(item))
}

type CmdQueryWindowValues C.Struct

func NewCmdQueryWindowValues(s *C.Segment) CmdQueryWindowValues {
	return CmdQueryWindowValues(s.NewStruct(40, 1))
}
func NewRootCmdQueryWindowValues(s *C.Segment) CmdQueryWindowValues {
	return CmdQueryWindowValues(s.NewRootStruct(40, 1))
}
func AutoNewCmdQueryWindowValues(s *C.Segment) CmdQueryWindowValues {
	return CmdQueryWindowValues(s.NewStructAR(40, 1))
}
func ReadRootCmdQueryWindowValues(s *C.Segment) CmdQueryWindowValues {
	return CmdQueryWindowValues(s.Root(0).ToStruct())
}
func (s CmdQueryWindowValues) Uuid() []byte         { return C.Struct(s).GetObject(0).ToData() }
func (s CmdQueryWindowValues) SetUuid(v []byte)     { C.Struct(s).SetObject(0, s.Segment.NewData(v)) }
func (s CmdQueryWindowValues) Version() uint64      { return C.Struct(s).Get64(0) }
func (s CmdQueryWindowValues) SetVersion(v uint64)  { C.Struct(s).Set64(0, v) }
func (s CmdQueryWindowValues) StartTime() int64     { return int64(C.Struct(s).Get64(8)) }
func (s CmdQueryWindowValues) SetStartTime(v int64) { C.Struct(s).Set64(8, uint64(v)) }
func (s CmdQueryWindowValues) EndTime() int64       { return int64(C.Struct(s).Get64(16)) }
func (s CmdQueryWindowValues) SetEndTime(v int64)   { C.Struct(s).Set64(16, uint64(v)) }
func (s CmdQueryWindowValues) Width() uint64        { return C.Struct(s).Get64(24) }
func (s CmdQueryWindowValues) SetWidth(v uint64)    { C.Struct(s).Set64(24, v) }
func (s CmdQueryWindowValues) Depth() uint8         { return C.Struct(s).Get8(32) }
func (s CmdQueryWindowValues) SetDepth(v uint8)     { C.Struct(s).Set8(32, v) }
func (s CmdQueryWindowValues) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"uuid\":")
	if err != nil {
		return err
	}
	{
		s := s.Uuid()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"version\":")
	if err != nil {
		return err
	}
	{
		s := s.Version()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"startTime\":")
	if err != nil {
		return err
	}
	{
		s := s.StartTime()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"endTime\":")
	if err != nil {
		return err
	}
	{
		s := s.EndTime()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"width\":")
	if err != nil {
		return err
	}
	{
		s := s.Width()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"depth\":")
	if err != nil {
		return err
	}
	{
		s := s.Depth()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s CmdQueryWindowValues) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s CmdQueryWindowValues) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("uuid = ")
	if err != nil {
		return err
	}
	{
		s := s.Uuid()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("version = ")
	if err != nil {
		return err
	}
	{
		s := s.Version()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("startTime = ")
	if err != nil {
		return err
	}
	{
		s := s.StartTime()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("endTime = ")
	if err != nil {
		return err
	}
	{
		s := s.EndTime()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("width = ")
	if err != nil {
		return err
	}
	{
		s := s.Width()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("depth = ")
	if err != nil {
		return err
	}
	{
		s := s.Depth()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s CmdQueryWindowValues) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type CmdQueryWindowValues_List C.PointerList

func NewCmdQueryWindowValuesList(s *C.Segment, sz int) CmdQueryWindowValues_List {
	return CmdQueryWindowValues_List(s.NewCompositeList(40, 1, sz))
}
func (s CmdQueryWindowValues_List) Len() int { return C.PointerList(s).Len() }
func (s CmdQueryWindowValues_List) At(i int) CmdQueryWindowValues {
	return CmdQueryWindowValues(C.PointerList(s).At(i).ToStruct())
}
func (s CmdQueryWindowValues_List) ToArray() []CmdQueryWindowValues {
	n := s.Len()
	a := make([]CmdQueryWindowValues, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s CmdQueryWindowValues_List) Set(i int, item CmdQueryWindowValues) {
	C.PointerList(s).Set(i, C.Object(item))
}

type CmdQueryVersion C.Struct

func NewCmdQueryVersion(s *C.Segment) CmdQueryVersion { return CmdQueryVersion(s.NewStruct(0, 1)) }
func NewRootCmdQueryVersion(s *C.Segment) CmdQueryVersion {
	return CmdQueryVersion(s.NewRootStruct(0, 1))
}
func AutoNewCmdQueryVersion(s *C.Segment) CmdQueryVersion { return CmdQueryVersion(s.NewStructAR(0, 1)) }
func ReadRootCmdQueryVersion(s *C.Segment) CmdQueryVersion {
	return CmdQueryVersion(s.Root(0).ToStruct())
}
func (s CmdQueryVersion) Uuids() C.DataList     { return C.DataList(C.Struct(s).GetObject(0)) }
func (s CmdQueryVersion) SetUuids(v C.DataList) { C.Struct(s).SetObject(0, C.Object(v)) }
func (s CmdQueryVersion) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"uuids\":")
	if err != nil {
		return err
	}
	{
		s := s.Uuids()
		{
			err = b.WriteByte('[')
			if err != nil {
				return err
			}
			for i, s := range s.ToArray() {
				if i != 0 {
					_, err = b.WriteString(", ")
				}
				if err != nil {
					return err
				}
				buf, err = json.Marshal(s)
				if err != nil {
					return err
				}
				_, err = b.Write(buf)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(']')
		}
		if err != nil {
			return err
		}
	}
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s CmdQueryVersion) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s CmdQueryVersion) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("uuids = ")
	if err != nil {
		return err
	}
	{
		s := s.Uuids()
		{
			err = b.WriteByte('[')
			if err != nil {
				return err
			}
			for i, s := range s.ToArray() {
				if i != 0 {
					_, err = b.WriteString(", ")
				}
				if err != nil {
					return err
				}
				buf, err = json.Marshal(s)
				if err != nil {
					return err
				}
				_, err = b.Write(buf)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(']')
		}
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s CmdQueryVersion) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
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
	n := s.Len()
	a := make([]CmdQueryVersion, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s CmdQueryVersion_List) Set(i int, item CmdQueryVersion) {
	C.PointerList(s).Set(i, C.Object(item))
}

type CmdQueryNearestValue C.Struct

func NewCmdQueryNearestValue(s *C.Segment) CmdQueryNearestValue {
	return CmdQueryNearestValue(s.NewStruct(24, 1))
}
func NewRootCmdQueryNearestValue(s *C.Segment) CmdQueryNearestValue {
	return CmdQueryNearestValue(s.NewRootStruct(24, 1))
}
func AutoNewCmdQueryNearestValue(s *C.Segment) CmdQueryNearestValue {
	return CmdQueryNearestValue(s.NewStructAR(24, 1))
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
func (s CmdQueryNearestValue) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"uuid\":")
	if err != nil {
		return err
	}
	{
		s := s.Uuid()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"version\":")
	if err != nil {
		return err
	}
	{
		s := s.Version()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"time\":")
	if err != nil {
		return err
	}
	{
		s := s.Time()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"backward\":")
	if err != nil {
		return err
	}
	{
		s := s.Backward()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s CmdQueryNearestValue) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s CmdQueryNearestValue) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("uuid = ")
	if err != nil {
		return err
	}
	{
		s := s.Uuid()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("version = ")
	if err != nil {
		return err
	}
	{
		s := s.Version()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("time = ")
	if err != nil {
		return err
	}
	{
		s := s.Time()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("backward = ")
	if err != nil {
		return err
	}
	{
		s := s.Backward()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s CmdQueryNearestValue) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
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
	n := s.Len()
	a := make([]CmdQueryNearestValue, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s CmdQueryNearestValue_List) Set(i int, item CmdQueryNearestValue) {
	C.PointerList(s).Set(i, C.Object(item))
}

type CmdQueryChangedRanges C.Struct

func NewCmdQueryChangedRanges(s *C.Segment) CmdQueryChangedRanges {
	return CmdQueryChangedRanges(s.NewStruct(32, 1))
}
func NewRootCmdQueryChangedRanges(s *C.Segment) CmdQueryChangedRanges {
	return CmdQueryChangedRanges(s.NewRootStruct(32, 1))
}
func AutoNewCmdQueryChangedRanges(s *C.Segment) CmdQueryChangedRanges {
	return CmdQueryChangedRanges(s.NewStructAR(32, 1))
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
func (s CmdQueryChangedRanges) Unused() uint64             { return C.Struct(s).Get64(16) }
func (s CmdQueryChangedRanges) SetUnused(v uint64)         { C.Struct(s).Set64(16, v) }
func (s CmdQueryChangedRanges) Resolution() uint8          { return C.Struct(s).Get8(24) }
func (s CmdQueryChangedRanges) SetResolution(v uint8)      { C.Struct(s).Set8(24, v) }
func (s CmdQueryChangedRanges) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"uuid\":")
	if err != nil {
		return err
	}
	{
		s := s.Uuid()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"fromGeneration\":")
	if err != nil {
		return err
	}
	{
		s := s.FromGeneration()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"toGeneration\":")
	if err != nil {
		return err
	}
	{
		s := s.ToGeneration()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"unused\":")
	if err != nil {
		return err
	}
	{
		s := s.Unused()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"resolution\":")
	if err != nil {
		return err
	}
	{
		s := s.Resolution()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s CmdQueryChangedRanges) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s CmdQueryChangedRanges) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("uuid = ")
	if err != nil {
		return err
	}
	{
		s := s.Uuid()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("fromGeneration = ")
	if err != nil {
		return err
	}
	{
		s := s.FromGeneration()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("toGeneration = ")
	if err != nil {
		return err
	}
	{
		s := s.ToGeneration()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("unused = ")
	if err != nil {
		return err
	}
	{
		s := s.Unused()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("resolution = ")
	if err != nil {
		return err
	}
	{
		s := s.Resolution()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s CmdQueryChangedRanges) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type CmdQueryChangedRanges_List C.PointerList

func NewCmdQueryChangedRangesList(s *C.Segment, sz int) CmdQueryChangedRanges_List {
	return CmdQueryChangedRanges_List(s.NewCompositeList(32, 1, sz))
}
func (s CmdQueryChangedRanges_List) Len() int { return C.PointerList(s).Len() }
func (s CmdQueryChangedRanges_List) At(i int) CmdQueryChangedRanges {
	return CmdQueryChangedRanges(C.PointerList(s).At(i).ToStruct())
}
func (s CmdQueryChangedRanges_List) ToArray() []CmdQueryChangedRanges {
	n := s.Len()
	a := make([]CmdQueryChangedRanges, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s CmdQueryChangedRanges_List) Set(i int, item CmdQueryChangedRanges) {
	C.PointerList(s).Set(i, C.Object(item))
}

type CmdInsertValues C.Struct

func NewCmdInsertValues(s *C.Segment) CmdInsertValues { return CmdInsertValues(s.NewStruct(8, 2)) }
func NewRootCmdInsertValues(s *C.Segment) CmdInsertValues {
	return CmdInsertValues(s.NewRootStruct(8, 2))
}
func AutoNewCmdInsertValues(s *C.Segment) CmdInsertValues { return CmdInsertValues(s.NewStructAR(8, 2)) }
func ReadRootCmdInsertValues(s *C.Segment) CmdInsertValues {
	return CmdInsertValues(s.Root(0).ToStruct())
}
func (s CmdInsertValues) Uuid() []byte            { return C.Struct(s).GetObject(0).ToData() }
func (s CmdInsertValues) SetUuid(v []byte)        { C.Struct(s).SetObject(0, s.Segment.NewData(v)) }
func (s CmdInsertValues) Values() Record_List     { return Record_List(C.Struct(s).GetObject(1)) }
func (s CmdInsertValues) SetValues(v Record_List) { C.Struct(s).SetObject(1, C.Object(v)) }
func (s CmdInsertValues) Sync() bool              { return C.Struct(s).Get1(0) }
func (s CmdInsertValues) SetSync(v bool)          { C.Struct(s).Set1(0, v) }
func (s CmdInsertValues) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"uuid\":")
	if err != nil {
		return err
	}
	{
		s := s.Uuid()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"values\":")
	if err != nil {
		return err
	}
	{
		s := s.Values()
		{
			err = b.WriteByte('[')
			if err != nil {
				return err
			}
			for i, s := range s.ToArray() {
				if i != 0 {
					_, err = b.WriteString(", ")
				}
				if err != nil {
					return err
				}
				err = s.WriteJSON(b)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(']')
		}
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"sync\":")
	if err != nil {
		return err
	}
	{
		s := s.Sync()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s CmdInsertValues) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s CmdInsertValues) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("uuid = ")
	if err != nil {
		return err
	}
	{
		s := s.Uuid()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("values = ")
	if err != nil {
		return err
	}
	{
		s := s.Values()
		{
			err = b.WriteByte('[')
			if err != nil {
				return err
			}
			for i, s := range s.ToArray() {
				if i != 0 {
					_, err = b.WriteString(", ")
				}
				if err != nil {
					return err
				}
				err = s.WriteCapLit(b)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(']')
		}
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("sync = ")
	if err != nil {
		return err
	}
	{
		s := s.Sync()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s CmdInsertValues) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
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
	n := s.Len()
	a := make([]CmdInsertValues, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s CmdInsertValues_List) Set(i int, item CmdInsertValues) {
	C.PointerList(s).Set(i, C.Object(item))
}

type CmdDeleteValues C.Struct

func NewCmdDeleteValues(s *C.Segment) CmdDeleteValues { return CmdDeleteValues(s.NewStruct(16, 1)) }
func NewRootCmdDeleteValues(s *C.Segment) CmdDeleteValues {
	return CmdDeleteValues(s.NewRootStruct(16, 1))
}
func AutoNewCmdDeleteValues(s *C.Segment) CmdDeleteValues {
	return CmdDeleteValues(s.NewStructAR(16, 1))
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
func (s CmdDeleteValues) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"uuid\":")
	if err != nil {
		return err
	}
	{
		s := s.Uuid()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"startTime\":")
	if err != nil {
		return err
	}
	{
		s := s.StartTime()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"endTime\":")
	if err != nil {
		return err
	}
	{
		s := s.EndTime()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s CmdDeleteValues) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s CmdDeleteValues) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("uuid = ")
	if err != nil {
		return err
	}
	{
		s := s.Uuid()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("startTime = ")
	if err != nil {
		return err
	}
	{
		s := s.StartTime()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("endTime = ")
	if err != nil {
		return err
	}
	{
		s := s.EndTime()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s CmdDeleteValues) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
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
	n := s.Len()
	a := make([]CmdDeleteValues, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s CmdDeleteValues_List) Set(i int, item CmdDeleteValues) {
	C.PointerList(s).Set(i, C.Object(item))
}

type Response C.Struct
type Response_Which uint16

const (
	RESPONSE_VOID               Response_Which = 0
	RESPONSE_RECORDS            Response_Which = 1
	RESPONSE_STATISTICALRECORDS Response_Which = 2
	RESPONSE_VERSIONLIST        Response_Which = 3
	RESPONSE_CHANGEDRNGLIST     Response_Which = 4
)

func NewResponse(s *C.Segment) Response       { return Response(s.NewStruct(16, 1)) }
func NewRootResponse(s *C.Segment) Response   { return Response(s.NewRootStruct(16, 1)) }
func AutoNewResponse(s *C.Segment) Response   { return Response(s.NewStructAR(16, 1)) }
func ReadRootResponse(s *C.Segment) Response  { return Response(s.Root(0).ToStruct()) }
func (s Response) Which() Response_Which      { return Response_Which(C.Struct(s).Get16(12)) }
func (s Response) EchoTag() uint64            { return C.Struct(s).Get64(0) }
func (s Response) SetEchoTag(v uint64)        { C.Struct(s).Set64(0, v) }
func (s Response) StatusCode() StatusCode     { return StatusCode(C.Struct(s).Get16(8)) }
func (s Response) SetStatusCode(v StatusCode) { C.Struct(s).Set16(8, uint16(v)) }
func (s Response) Final() bool                { return C.Struct(s).Get1(80) }
func (s Response) SetFinal(v bool)            { C.Struct(s).Set1(80, v) }
func (s Response) SetVoid()                   { C.Struct(s).Set16(12, 0) }
func (s Response) Records() Records           { return Records(C.Struct(s).GetObject(0).ToStruct()) }
func (s Response) SetRecords(v Records) {
	C.Struct(s).Set16(12, 1)
	C.Struct(s).SetObject(0, C.Object(v))
}
func (s Response) StatisticalRecords() StatisticalRecords {
	return StatisticalRecords(C.Struct(s).GetObject(0).ToStruct())
}
func (s Response) SetStatisticalRecords(v StatisticalRecords) {
	C.Struct(s).Set16(12, 2)
	C.Struct(s).SetObject(0, C.Object(v))
}
func (s Response) VersionList() Versions { return Versions(C.Struct(s).GetObject(0).ToStruct()) }
func (s Response) SetVersionList(v Versions) {
	C.Struct(s).Set16(12, 3)
	C.Struct(s).SetObject(0, C.Object(v))
}
func (s Response) ChangedRngList() Ranges { return Ranges(C.Struct(s).GetObject(0).ToStruct()) }
func (s Response) SetChangedRngList(v Ranges) {
	C.Struct(s).Set16(12, 4)
	C.Struct(s).SetObject(0, C.Object(v))
}
func (s Response) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"echoTag\":")
	if err != nil {
		return err
	}
	{
		s := s.EchoTag()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"statusCode\":")
	if err != nil {
		return err
	}
	{
		s := s.StatusCode()
		err = s.WriteJSON(b)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"final\":")
	if err != nil {
		return err
	}
	{
		s := s.Final()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	if s.Which() == RESPONSE_VOID {
		_, err = b.WriteString("\"void\":")
		if err != nil {
			return err
		}
		_ = s
		_, err = b.WriteString("null")
		if err != nil {
			return err
		}
	}
	if s.Which() == RESPONSE_RECORDS {
		_, err = b.WriteString("\"records\":")
		if err != nil {
			return err
		}
		{
			s := s.Records()
			err = s.WriteJSON(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == RESPONSE_STATISTICALRECORDS {
		_, err = b.WriteString("\"statisticalRecords\":")
		if err != nil {
			return err
		}
		{
			s := s.StatisticalRecords()
			err = s.WriteJSON(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == RESPONSE_VERSIONLIST {
		_, err = b.WriteString("\"versionList\":")
		if err != nil {
			return err
		}
		{
			s := s.VersionList()
			err = s.WriteJSON(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == RESPONSE_CHANGEDRNGLIST {
		_, err = b.WriteString("\"changedRngList\":")
		if err != nil {
			return err
		}
		{
			s := s.ChangedRngList()
			err = s.WriteJSON(b)
			if err != nil {
				return err
			}
		}
	}
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s Response) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s Response) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("echoTag = ")
	if err != nil {
		return err
	}
	{
		s := s.EchoTag()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("statusCode = ")
	if err != nil {
		return err
	}
	{
		s := s.StatusCode()
		err = s.WriteCapLit(b)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("final = ")
	if err != nil {
		return err
	}
	{
		s := s.Final()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	if s.Which() == RESPONSE_VOID {
		_, err = b.WriteString("void = ")
		if err != nil {
			return err
		}
		_ = s
		_, err = b.WriteString("null")
		if err != nil {
			return err
		}
	}
	if s.Which() == RESPONSE_RECORDS {
		_, err = b.WriteString("records = ")
		if err != nil {
			return err
		}
		{
			s := s.Records()
			err = s.WriteCapLit(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == RESPONSE_STATISTICALRECORDS {
		_, err = b.WriteString("statisticalRecords = ")
		if err != nil {
			return err
		}
		{
			s := s.StatisticalRecords()
			err = s.WriteCapLit(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == RESPONSE_VERSIONLIST {
		_, err = b.WriteString("versionList = ")
		if err != nil {
			return err
		}
		{
			s := s.VersionList()
			err = s.WriteCapLit(b)
			if err != nil {
				return err
			}
		}
	}
	if s.Which() == RESPONSE_CHANGEDRNGLIST {
		_, err = b.WriteString("changedRngList = ")
		if err != nil {
			return err
		}
		{
			s := s.ChangedRngList()
			err = s.WriteCapLit(b)
			if err != nil {
				return err
			}
		}
	}
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s Response) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type Response_List C.PointerList

func NewResponseList(s *C.Segment, sz int) Response_List {
	return Response_List(s.NewCompositeList(16, 1, sz))
}
func (s Response_List) Len() int          { return C.PointerList(s).Len() }
func (s Response_List) At(i int) Response { return Response(C.PointerList(s).At(i).ToStruct()) }
func (s Response_List) ToArray() []Response {
	n := s.Len()
	a := make([]Response, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s Response_List) Set(i int, item Response) { C.PointerList(s).Set(i, C.Object(item)) }

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

func StatusCodeFromString(c string) StatusCode {
	switch c {
	case "ok":
		return STATUSCODE_OK
	case "internalError":
		return STATUSCODE_INTERNALERROR
	case "noSuchStreamOrVersion":
		return STATUSCODE_NOSUCHSTREAMORVERSION
	case "invalidParameter":
		return STATUSCODE_INVALIDPARAMETER
	case "noSuchPoint":
		return STATUSCODE_NOSUCHPOINT
	default:
		return 0
	}
}

type StatusCode_List C.PointerList

func NewStatusCodeList(s *C.Segment, sz int) StatusCode_List {
	return StatusCode_List(s.NewUInt16List(sz))
}
func (s StatusCode_List) Len() int            { return C.UInt16List(s).Len() }
func (s StatusCode_List) At(i int) StatusCode { return StatusCode(C.UInt16List(s).At(i)) }
func (s StatusCode_List) ToArray() []StatusCode {
	n := s.Len()
	a := make([]StatusCode, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s StatusCode_List) Set(i int, item StatusCode) { C.UInt16List(s).Set(i, uint16(item)) }
func (s StatusCode) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	buf, err = json.Marshal(s.String())
	if err != nil {
		return err
	}
	_, err = b.Write(buf)
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s StatusCode) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s StatusCode) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	_, err = b.WriteString(s.String())
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s StatusCode) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type Records C.Struct

func NewRecords(s *C.Segment) Records      { return Records(s.NewStruct(8, 1)) }
func NewRootRecords(s *C.Segment) Records  { return Records(s.NewRootStruct(8, 1)) }
func AutoNewRecords(s *C.Segment) Records  { return Records(s.NewStructAR(8, 1)) }
func ReadRootRecords(s *C.Segment) Records { return Records(s.Root(0).ToStruct()) }
func (s Records) Version() uint64          { return C.Struct(s).Get64(0) }
func (s Records) SetVersion(v uint64)      { C.Struct(s).Set64(0, v) }
func (s Records) Values() Record_List      { return Record_List(C.Struct(s).GetObject(0)) }
func (s Records) SetValues(v Record_List)  { C.Struct(s).SetObject(0, C.Object(v)) }
func (s Records) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"version\":")
	if err != nil {
		return err
	}
	{
		s := s.Version()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"values\":")
	if err != nil {
		return err
	}
	{
		s := s.Values()
		{
			err = b.WriteByte('[')
			if err != nil {
				return err
			}
			for i, s := range s.ToArray() {
				if i != 0 {
					_, err = b.WriteString(", ")
				}
				if err != nil {
					return err
				}
				err = s.WriteJSON(b)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(']')
		}
		if err != nil {
			return err
		}
	}
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s Records) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s Records) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("version = ")
	if err != nil {
		return err
	}
	{
		s := s.Version()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("values = ")
	if err != nil {
		return err
	}
	{
		s := s.Values()
		{
			err = b.WriteByte('[')
			if err != nil {
				return err
			}
			for i, s := range s.ToArray() {
				if i != 0 {
					_, err = b.WriteString(", ")
				}
				if err != nil {
					return err
				}
				err = s.WriteCapLit(b)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(']')
		}
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s Records) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type Records_List C.PointerList

func NewRecordsList(s *C.Segment, sz int) Records_List {
	return Records_List(s.NewCompositeList(8, 1, sz))
}
func (s Records_List) Len() int         { return C.PointerList(s).Len() }
func (s Records_List) At(i int) Records { return Records(C.PointerList(s).At(i).ToStruct()) }
func (s Records_List) ToArray() []Records {
	n := s.Len()
	a := make([]Records, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s Records_List) Set(i int, item Records) { C.PointerList(s).Set(i, C.Object(item)) }

type StatisticalRecords C.Struct

func NewStatisticalRecords(s *C.Segment) StatisticalRecords {
	return StatisticalRecords(s.NewStruct(8, 1))
}
func NewRootStatisticalRecords(s *C.Segment) StatisticalRecords {
	return StatisticalRecords(s.NewRootStruct(8, 1))
}
func AutoNewStatisticalRecords(s *C.Segment) StatisticalRecords {
	return StatisticalRecords(s.NewStructAR(8, 1))
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
func (s StatisticalRecords) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"version\":")
	if err != nil {
		return err
	}
	{
		s := s.Version()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"values\":")
	if err != nil {
		return err
	}
	{
		s := s.Values()
		{
			err = b.WriteByte('[')
			if err != nil {
				return err
			}
			for i, s := range s.ToArray() {
				if i != 0 {
					_, err = b.WriteString(", ")
				}
				if err != nil {
					return err
				}
				err = s.WriteJSON(b)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(']')
		}
		if err != nil {
			return err
		}
	}
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s StatisticalRecords) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s StatisticalRecords) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("version = ")
	if err != nil {
		return err
	}
	{
		s := s.Version()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("values = ")
	if err != nil {
		return err
	}
	{
		s := s.Values()
		{
			err = b.WriteByte('[')
			if err != nil {
				return err
			}
			for i, s := range s.ToArray() {
				if i != 0 {
					_, err = b.WriteString(", ")
				}
				if err != nil {
					return err
				}
				err = s.WriteCapLit(b)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(']')
		}
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s StatisticalRecords) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
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
	n := s.Len()
	a := make([]StatisticalRecords, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s StatisticalRecords_List) Set(i int, item StatisticalRecords) {
	C.PointerList(s).Set(i, C.Object(item))
}

type Versions C.Struct

func NewVersions(s *C.Segment) Versions       { return Versions(s.NewStruct(0, 2)) }
func NewRootVersions(s *C.Segment) Versions   { return Versions(s.NewRootStruct(0, 2)) }
func AutoNewVersions(s *C.Segment) Versions   { return Versions(s.NewStructAR(0, 2)) }
func ReadRootVersions(s *C.Segment) Versions  { return Versions(s.Root(0).ToStruct()) }
func (s Versions) Uuids() C.DataList          { return C.DataList(C.Struct(s).GetObject(0)) }
func (s Versions) SetUuids(v C.DataList)      { C.Struct(s).SetObject(0, C.Object(v)) }
func (s Versions) Versions() C.UInt64List     { return C.UInt64List(C.Struct(s).GetObject(1)) }
func (s Versions) SetVersions(v C.UInt64List) { C.Struct(s).SetObject(1, C.Object(v)) }
func (s Versions) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"uuids\":")
	if err != nil {
		return err
	}
	{
		s := s.Uuids()
		{
			err = b.WriteByte('[')
			if err != nil {
				return err
			}
			for i, s := range s.ToArray() {
				if i != 0 {
					_, err = b.WriteString(", ")
				}
				if err != nil {
					return err
				}
				buf, err = json.Marshal(s)
				if err != nil {
					return err
				}
				_, err = b.Write(buf)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(']')
		}
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"versions\":")
	if err != nil {
		return err
	}
	{
		s := s.Versions()
		{
			err = b.WriteByte('[')
			if err != nil {
				return err
			}
			for i, s := range s.ToArray() {
				if i != 0 {
					_, err = b.WriteString(", ")
				}
				if err != nil {
					return err
				}
				buf, err = json.Marshal(s)
				if err != nil {
					return err
				}
				_, err = b.Write(buf)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(']')
		}
		if err != nil {
			return err
		}
	}
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s Versions) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s Versions) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("uuids = ")
	if err != nil {
		return err
	}
	{
		s := s.Uuids()
		{
			err = b.WriteByte('[')
			if err != nil {
				return err
			}
			for i, s := range s.ToArray() {
				if i != 0 {
					_, err = b.WriteString(", ")
				}
				if err != nil {
					return err
				}
				buf, err = json.Marshal(s)
				if err != nil {
					return err
				}
				_, err = b.Write(buf)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(']')
		}
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("versions = ")
	if err != nil {
		return err
	}
	{
		s := s.Versions()
		{
			err = b.WriteByte('[')
			if err != nil {
				return err
			}
			for i, s := range s.ToArray() {
				if i != 0 {
					_, err = b.WriteString(", ")
				}
				if err != nil {
					return err
				}
				buf, err = json.Marshal(s)
				if err != nil {
					return err
				}
				_, err = b.Write(buf)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(']')
		}
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s Versions) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type Versions_List C.PointerList

func NewVersionsList(s *C.Segment, sz int) Versions_List {
	return Versions_List(s.NewCompositeList(0, 2, sz))
}
func (s Versions_List) Len() int          { return C.PointerList(s).Len() }
func (s Versions_List) At(i int) Versions { return Versions(C.PointerList(s).At(i).ToStruct()) }
func (s Versions_List) ToArray() []Versions {
	n := s.Len()
	a := make([]Versions, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s Versions_List) Set(i int, item Versions) { C.PointerList(s).Set(i, C.Object(item)) }

type ChangedRange C.Struct

func NewChangedRange(s *C.Segment) ChangedRange      { return ChangedRange(s.NewStruct(16, 0)) }
func NewRootChangedRange(s *C.Segment) ChangedRange  { return ChangedRange(s.NewRootStruct(16, 0)) }
func AutoNewChangedRange(s *C.Segment) ChangedRange  { return ChangedRange(s.NewStructAR(16, 0)) }
func ReadRootChangedRange(s *C.Segment) ChangedRange { return ChangedRange(s.Root(0).ToStruct()) }
func (s ChangedRange) StartTime() int64              { return int64(C.Struct(s).Get64(0)) }
func (s ChangedRange) SetStartTime(v int64)          { C.Struct(s).Set64(0, uint64(v)) }
func (s ChangedRange) EndTime() int64                { return int64(C.Struct(s).Get64(8)) }
func (s ChangedRange) SetEndTime(v int64)            { C.Struct(s).Set64(8, uint64(v)) }
func (s ChangedRange) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"startTime\":")
	if err != nil {
		return err
	}
	{
		s := s.StartTime()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"endTime\":")
	if err != nil {
		return err
	}
	{
		s := s.EndTime()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s ChangedRange) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s ChangedRange) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("startTime = ")
	if err != nil {
		return err
	}
	{
		s := s.StartTime()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("endTime = ")
	if err != nil {
		return err
	}
	{
		s := s.EndTime()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s ChangedRange) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
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
	n := s.Len()
	a := make([]ChangedRange, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s ChangedRange_List) Set(i int, item ChangedRange) { C.PointerList(s).Set(i, C.Object(item)) }

type Ranges C.Struct

func NewRanges(s *C.Segment) Ranges            { return Ranges(s.NewStruct(8, 1)) }
func NewRootRanges(s *C.Segment) Ranges        { return Ranges(s.NewRootStruct(8, 1)) }
func AutoNewRanges(s *C.Segment) Ranges        { return Ranges(s.NewStructAR(8, 1)) }
func ReadRootRanges(s *C.Segment) Ranges       { return Ranges(s.Root(0).ToStruct()) }
func (s Ranges) Version() uint64               { return C.Struct(s).Get64(0) }
func (s Ranges) SetVersion(v uint64)           { C.Struct(s).Set64(0, v) }
func (s Ranges) Values() ChangedRange_List     { return ChangedRange_List(C.Struct(s).GetObject(0)) }
func (s Ranges) SetValues(v ChangedRange_List) { C.Struct(s).SetObject(0, C.Object(v)) }
func (s Ranges) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"version\":")
	if err != nil {
		return err
	}
	{
		s := s.Version()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"values\":")
	if err != nil {
		return err
	}
	{
		s := s.Values()
		{
			err = b.WriteByte('[')
			if err != nil {
				return err
			}
			for i, s := range s.ToArray() {
				if i != 0 {
					_, err = b.WriteString(", ")
				}
				if err != nil {
					return err
				}
				err = s.WriteJSON(b)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(']')
		}
		if err != nil {
			return err
		}
	}
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s Ranges) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s Ranges) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("version = ")
	if err != nil {
		return err
	}
	{
		s := s.Version()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("values = ")
	if err != nil {
		return err
	}
	{
		s := s.Values()
		{
			err = b.WriteByte('[')
			if err != nil {
				return err
			}
			for i, s := range s.ToArray() {
				if i != 0 {
					_, err = b.WriteString(", ")
				}
				if err != nil {
					return err
				}
				err = s.WriteCapLit(b)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(']')
		}
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s Ranges) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type Ranges_List C.PointerList

func NewRangesList(s *C.Segment, sz int) Ranges_List { return Ranges_List(s.NewCompositeList(8, 1, sz)) }
func (s Ranges_List) Len() int                       { return C.PointerList(s).Len() }
func (s Ranges_List) At(i int) Ranges                { return Ranges(C.PointerList(s).At(i).ToStruct()) }
func (s Ranges_List) ToArray() []Ranges {
	n := s.Len()
	a := make([]Ranges, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s Ranges_List) Set(i int, item Ranges) { C.PointerList(s).Set(i, C.Object(item)) }
