package rez

//mercy is a rez manager. There exists a small static number of resource pools.
//These pools contain resource handles that allow the program to do certain actions.
//The pool size and maximum number of goroutines queueing for a handle is defined
//in a cluster-wide configuration. If your context expires while you own a resource
//it is up to you to properly release it. If your context expires while you are in
//the queue, your channel will get closed.

import (
	"context"
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/SoftwareDefinedBuildings/btrdb/bte"
	logging "github.com/op/go-logging"
	opentracing "github.com/opentracing/opentracing-go"
	tlog "github.com/opentracing/opentracing-go/log"
)

var log *logging.Logger

//if true, long held resources will have their stacks printed to console
const traceResources = true
const traceThresh = 10 * time.Second

func init() {
	log = logging.MustGetLogger("log")
}

type ResourceIdentifier string

type TunableProvider interface {
	// Get a tunable, and watch it for all future changes. Only meant to be
	// used by Rez
	WatchTunable(name string, onchange func(v string)) error
}

const CephHotHandle ResourceIdentifier = "ceph_hot"
const CephColdHandle ResourceIdentifier = "ceph_cold"
const ConcurrentOp ResourceIdentifier = "concurrent_op"
const OpenTrees ResourceIdentifier = "open_trees"
const OpenReadTrees ResourceIdentifier = "open_read_trees"
const MaximumConnections ResourceIdentifier = "max_conns"

const defaultMaxQueue = 100

type RezManager struct {
	cfg   TunableProvider
	pools map[ResourceIdentifier]*resourcePool
	mu    sync.Mutex
}

type queueEntry struct {
	ctx context.Context
	ch  chan *Resource
}
type Resource struct {
	v         interface{}
	available bool
	pool      *resourcePool
	span      opentracing.Span
	stack     string
	obtain    time.Time
}

type resourcePool struct {
	id      ResourceIdentifier
	newfunc func() interface{}
	delfunc func(v interface{})

	//parameters from config
	maxq    int
	desired int

	//book keeping
	available int

	pool  []*Resource
	queue []*queueEntry
	mu    sync.Mutex
}

func (r *Resource) Val() interface{} {
	return r.v
}

func NewResourceManager(cfg TunableProvider) *RezManager {
	return &RezManager{
		cfg:   cfg,
		pools: make(map[ResourceIdentifier]*resourcePool),
	}
}

func NopNew() interface{} {
	return nil
}
func NopDel(v interface{}) {

}
func (rez *RezManager) CreateResourcePool(id ResourceIdentifier,
	newfunc func() interface{},
	delfunc func(v interface{})) {
	rpool := &resourcePool{id: id, newfunc: newfunc, delfunc: delfunc, maxq: defaultMaxQueue}
	go func() {
		for {
			time.Sleep(1 * time.Minute)
			rpool.mu.Lock()
			rpool.lockHeldCleanQueue()
			rpool.mu.Unlock()
		}
	}()
	if traceResources {
		go func() {
			for {
				time.Sleep(5 * time.Second)
				log.Infof("pool %s has %d available and a queue of %d\n", id, rpool.available, len(rpool.queue))
				rpool.mu.Lock()
				for _, res := range rpool.pool {
					if res.available {
						continue
					}
					delta := time.Now().Sub(res.obtain)
					if delta > traceThresh {
						log.Infof("long held resource %s (%s) stack:\n%s\n---", id, delta, res.stack)
						fmt.Printf("stack: %s\n", res.stack)
					}
				}
				rpool.mu.Unlock()
			}
		}()
	}
	rez.mu.Lock()
	_, ok := rez.pools[id]
	if ok {
		panic("duplicate resource pool")
	}
	rez.pools[id] = rpool
	rez.mu.Unlock()

	rez.cfg.WatchTunable(string(id), func(val string) {
		parts := strings.Split(val, ",")
		if len(parts) != 2 {
			panic("bad resource tuning")
		}
		amount, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			panic("bad resource tuning")
		}
		maxq, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			panic("bad resource tuning")
		}
		log.Noticef("adjusting tunable '%s' to %d q=%d", id, amount, maxq)
		rpool.AdjustTuning(int(amount), int(maxq))
	})
}

func (rez *RezManager) Get(ctx context.Context, id ResourceIdentifier) (*Resource, bte.BTE) {
	return rez.get(ctx, id, true)
}

func (rez *RezManager) MustGet(ctx context.Context, id ResourceIdentifier) *Resource {
	r, err := rez.get(ctx, id, false)
	if err != nil {
		panic("inconceivable")
	}
	return r
}

func (rez *RezManager) get(ctx context.Context, id ResourceIdentifier, canfail bool) (*Resource, bte.BTE) {
	rez.mu.Lock()
	p, ok := rez.pools[id]
	rez.mu.Unlock()
	if !ok {
		panic("unknown resource")
	}
	r, err := p.Obtain(ctx, true)
	return r, err
}

func (p *resourcePool) AdjustTuning(desired int, maxq int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	//This is fine, we don't evict existing
	p.maxq = maxq
	p.desired = desired
	//This is easy, make more
	if len(p.pool) < desired {
		for i := len(p.pool); i < desired; i++ {
			newr := &Resource{available: true, v: p.newfunc(), pool: p}
			p.available++
			p.pool = append(p.pool, newr)
		}
	}
	//for shrinking down, it will be done by resource release
}

func (r *Resource) Release() {
	if r == nil {
		panic("release of nil resource")
	}
	if r.available {
		panic("release of available resource")
	}
	r.pool.mu.Lock()
	defer r.pool.mu.Unlock()
	r.span.Finish()
	if len(r.pool.pool) > r.pool.desired {
		//We need to destroy this resource from the pool
		r.pool.lockHeldDestroy(r)
	} else {
		//Check if we pass this resource to someone in the
		//queue. Drop all expired contexts we come across in the queue
		for len(r.pool.queue) != 0 {
			winner := r.pool.queue[0]
			r.pool.queue = r.pool.queue[1:]
			if winner.ctx.Err() == nil {
				winner.ch <- r
				return
			} else {
				close(winner.ch)
				log.Infof("dropped expired resource request in pool %s", r.pool.id)
			}
		}
		//Ok it goes back into the pool
		r.available = true
		r.stack = "released"
		r.pool.available++
	}
}
func (p *resourcePool) lockHeldCleanQueue() {
	exp := 0
	for _, ent := range p.queue {
		if ent.ctx.Err() != nil {
			exp++
		}
	}
	if exp == 0 {
		return
	}
	newq := make([]*queueEntry, 0, len(p.queue)-exp)
	for _, ent := range p.queue {
		if ent.ctx.Err() != nil {
			close(ent.ch)
			log.Infof("dropped expired resource request %s", p.id)
		} else {
			newq = append(newq, ent)
		}
	}
	p.queue = newq
}

func (p *resourcePool) lockHeldDestroy(r *Resource) {
	//THE MUTEX MUST BE HELD WHEN YOU CALL THIS
	newpool := make([]*Resource, len(p.pool)-1)
	idx := 0
	for _, oldres := range p.pool {
		if oldres == r {
			continue
		}
		newpool[idx] = oldres
		idx++
	}
	p.delfunc(r.Val())
}
func (p *resourcePool) Obtain(ctx context.Context, canfail bool) (*Resource, bte.BTE) {
	span, ctx := opentracing.StartSpanFromContext(ctx, fmt.Sprintf("Rez.%s", string(p.id)))
	ospan := opentracing.StartSpan(
		fmt.Sprintf("rez.obtain.%s", string(p.id)),
		opentracing.ChildOf(span.Context()))
	defer ospan.Finish()
	p.mu.Lock()
	if p.available > 0 {
		for _, r := range p.pool {
			if r.available {
				r.available = false
				if traceResources {
					r.stack = string(debug.Stack())
					r.obtain = time.Now()
				}
				r.span = span
				p.available--
				p.mu.Unlock()
				return r, nil
			}
		}
	}

	//Before we fail, make sure we clean
	if len(p.queue) >= p.maxq {
		p.lockHeldCleanQueue()
	}
	//None available, are we allowed to queue?
	if canfail && len(p.queue) >= p.maxq {
		p.mu.Unlock()
		log.Warningf("shedding resource load on %s", p.id)
		span.LogFields(tlog.String("event", "depleted"))
		return nil, bte.Err(bte.ResourceDepleted, "The cluster is overwhelmed and is shedding load.")
	}

	//Ok we are going into the queue
	us := make(chan *Resource, 1)
	p.queue = append(p.queue, &queueEntry{ctx: ctx, ch: us})
	p.mu.Unlock()

	rv := <-us
	if rv == nil {
		if ctx.Err() == nil {
			panic("didn't expect this")
		}
		span.LogFields(tlog.String("event", "expired"))
		span.Finish()
		return nil, bte.ErrW(bte.ContextError, "context error while obtaining resource", ctx.Err())
	}
	if traceResources {
		rv.stack = string(debug.Stack())
		rv.obtain = time.Now()
	}
	rv.span = span
	return rv, nil

}
