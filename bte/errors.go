package bte

import (
	"fmt"
	"os"
	"runtime/debug"

	"golang.org/x/net/context"
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
	if bte.Cause == nil {
		return fmt.Sprintf("(%d: %s)", bte.code, bte.reason)
	} else {
		return fmt.Sprintf("(%d: %s because %s)", bte.code, bte.reason, bte.cause.Error())
	}
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
	return &bTE{
		code:   code,
		reason: reason,
		cause:  cause,
	}
}
func CtxE(ctx context.Context) BTE {
	return &bTE{
		code:   ContextError,
		reason: "context error",
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

//Used for assert statements
const InvariantFailure = 500
