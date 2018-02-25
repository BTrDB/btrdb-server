package bte

import (
	"fmt"
	"os"
	"runtime/debug"

	"context"
)

// This defines all the errors that BTrDB can throw
type bTE struct {
	code   int
	reason string
	cause  error
}

type BTE interface {
	error
	Code() int
	Reason() string
	Cause() error
}

func (bte *bTE) Code() int {
	return bte.code
}

func (bte *bTE) Reason() string {
	return bte.reason
}

func (bte *bTE) Cause() error {
	return bte.cause
}

func (bte *bTE) WrappedErrors() []error {
	return []error{bte.cause}
}
func (bte *bTE) Error() string {
	if bte.cause == nil {
		return fmt.Sprintf("(%d: %s)", bte.code, bte.reason)
	}
	return fmt.Sprintf("(%d: %s because %s)", bte.code, bte.reason, bte.cause.Error())

}

func MaybeWrap(err error) BTE {
	bt, ok := err.(BTE)
	if ok {
		return bt
	}
	return Err(GenericError, err.Error())
}

// Error codes:
// 400+ normal user errors
// 500+ abnormal errors that sysadmin should be notified about

func Err(code int, reason string) BTE {
	if code >= 500 {
		fmt.Fprintf(os.Stderr, "\n\n=== %d code error ===\nreason: %s\n", code, reason)
		debug.PrintStack()
		fmt.Fprintf(os.Stderr, "====\n\n")
	}
	return &bTE{
		code:   code,
		reason: reason,
		cause:  nil,
	}
}
func ErrF(code int, reasonz string, args ...interface{}) BTE {
	reason := fmt.Sprintf(reasonz, args...)
	if code >= 500 {
		fmt.Fprintf(os.Stderr, "\n\n=== %d code error ===\nreason: %s\n", code, reason)
		debug.PrintStack()
		fmt.Fprintf(os.Stderr, "====\n\n")
	}
	return &bTE{
		code:   code,
		reason: reason,
		cause:  nil,
	}
}
func ErrW(code int, reason string, cause error) BTE {
	if code >= 500 {
		fmt.Fprintf(os.Stderr, "\n\n=== %d code error ===\nreason: %s\nbecause: %s", code, reason, cause.Error())
		debug.PrintStack()
		fmt.Fprintf(os.Stderr, "====\n\n")
	}
	scause := "<nil>"
	if cause != nil {
		scause = cause.Error()
	}
	return &bTE{
		code:   code,
		reason: fmt.Sprintf("%s (%s)", reason, scause),
		cause:  cause,
	}
}
func CtxE(ctx context.Context) BTE {
	return &bTE{
		code:   ContextError,
		reason: fmt.Sprintf("%s (%s)", "context error", ctx.Err()),
		cause:  ctx.Err(),
	}
}
func Chan(e BTE) chan BTE {
	rv := make(chan BTE, 1)
	rv <- e //buffered
	return rv
}

//Context errors cascade quite a bit and tend to cause duplicate errors
//in the return channel. Try not to leak goroutiens by]
//blocking on them
func ChkContextError(ctx context.Context, rve chan BTE) bool {
	if ctx.Err() != nil {
		select {
		case rve <- CtxE(ctx):
		default:
		}
		return true
	}
	return false
}
func NoBlockError(e BTE, ch chan BTE) {
	if e != nil {
		select {
		case ch <- e:
		default:
		}
	}
}

// If you ask for next/prev point but there isn't one
const NoSuchPoint = 401

// Things like user timeout
const ContextError = 402

// Like tree depth
const InsertFailure = 403

const NoSuchStream = 404

// We don't have a write lock for this stream
const WrongEndpoint = 405

// The stream already exists
const StreamExists = 406

// Collection name is invalid
const InvalidCollection = 407

// Tag key is invalid
const InvalidTagKey = 408

// Tag value is invalid
const InvalidTagValue = 409

// Just in case
const AwesomenessExceedsThreshold = 410

// For commands accepting a limit argument, the passed limit is invalid
const InvalidLimit = 411

// If a set of tags is given to identify a single stream, but fails to do
// so
const AmbiguousTags = 412

// The start/end times are invalid
const InvalidTimeRange = 413

// The insertion is too big (that's what she said)
const InsertTooBig = 414

// Point widths are [0, 64)
const InvalidPointWidth = 415

// When an error has no code
const GenericError = 416

// When create() is called and the uuid and tags are the same
const SameStream = 417

// When create() is called and although the uuid is different, the tags are not unique
const AmbiguousStream = 418

// When a write op on an unmapped UUID is attempted
const ClusterDegraded = 419

// Just in case this is required after Prop 64
const BlazeIt = 420

// Generated in drivers when the arguments are the wrong type or length
const WrongArgs = 421

// Annotations cannot exceed 2MB
const AnnotationTooBig = 422

// The annotation version did not match
const AnnotationVersionMismatch = 423

// Fault injection is disabled (you need $BTRDB_ENABLE_FAULT_INJECTON=YES)
const FaultInjectionDisabled = 424

// Returned when you try insert NaN or Inf values
const BadValue = 425

// Used when the cluster is extremely overloaded and is shedding load to
// maintain stability
const ResourceDepleted = 426

// Returned when you try do changed ranges with bad versions
const InvalidVersions = 427

// Returned when the etcd cluster is unhealthy
const EtcdFailure = 428

// Returned if you try create a stream with a uuid that is the same as a
// previously deleted stream
const ReusedUUID = 429

// Returned if you try delete a stream while its attributes are being
// modified
const ConcurrentModification = 430

// Returned if you try to obliterate a stream on a node where
// BTRDB_ENABLE_OBLITERATE is not set to "YES"
const ObliterateDisabled = 431

// Generic error from ceph
const CephError = 432

// Means that a node ID is reused, as of BTrDB 4.8 a node id must be unique
const NodeExisted = 433

// Generic error returned when  journalling
const JournalError = 434

// Used for assert statements
const InvariantFailure = 500

// Haha lol
const NotImplemented = 501
