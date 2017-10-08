package configprovider

import (
	"fmt"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	client "github.com/coreos/etcd/clientv3"
	"github.com/huichen/murmur"
	_ "github.com/zhangxinngang/murmur"
	"golang.org/x/net/context"
)

type cman struct {
	mu           sync.Mutex
	watchers     []func(chan bool)
	aliveLeaseID client.LeaseID
	//	lastClusterState *ClusterState
	ctx       context.Context
	ctxCancel func()
	nodehash  uint32
	//This is the value of the mash that the rest of BTrDB is up to date with
	notifiedMashNum int64
	faulted         bool

	//These are the mash ranges that we use for determining if we have a lock
	//or not. They come from the notifiedMsahNum
	ourNotifiedStart int64
	ourNotifiedEnd   int64
	notifiedRangeMu  sync.RWMutex

	cachedStateMu sync.Mutex
	cachedState   *ClusterState

	weExpectToBeUp bool
}

const True = "true"

type Member struct {
	Nodename string
	Enabled  bool
	In       bool
	Hash     uint32
	//Zero if not present, but also if present and zero (during startup)
	Active                  int64
	Weight                  int64
	ReadWeight              float64
	AdvertisedEndpointsHTTP []string
	AdvertisedEndpointsGRPC []string
}

type MASHMap struct {
	Ranges      []*MashRange
	Hashes      []uint32
	Nodenames   []string
	Weights     []int64
	TotalWeight int64
	c           *etcdconfig
}

func (m *Member) IsIn() bool {
	return m.In && m.Enabled && m.Active != 0 && m.Weight != 0
}

const HASHRANGE_END = (1 << 32)

type ClusterState struct {
	Revision       int64
	Members        map[string]*Member
	Mashes         map[int64]map[string]*MashRange
	Leader         string
	LeaderRevision int64
	c              *etcdconfig
}
type MashRange struct {
	Start int64
	End   int64
}

func (c *etcdconfig) Faulted() bool {
	return c.faulted
}
func (c *etcdconfig) GetCachedClusterState() *ClusterState {
	c.cachedStateMu.Lock()
	rv := c.cachedState
	c.cachedStateMu.Unlock()
	return rv
}
func (c *etcdconfig) trace(fmts string, args ...interface{}) {
	fmt.Printf("$$ " + c.nodename + " " + fmt.Sprintf(fmts, args...) + "\n")
}

func (cs *ClusterState) Healthy() bool {
	_, _, all := cs.ProposedMashNumber()
	cm := cs.ActiveMASH()
	//fmt.Println("Current mash:\n", cm.String())
	g := cm.Gap()
	return all && g == 0
}
func (cs *ClusterState) GapPercentage() float64 {
	return float64(cs.ActiveMASH().Gap()) * 100 / float64(HASHRANGE_END)
}
func (cs *ClusterState) String() string {
	ldr := "--"
	if cs.Leader != "" {
		ldr = cs.Leader
	}
	clusterstate := "?"
	proposedNum, activeNum, all := cs.ProposedMashNumber()
	cm := cs.ActiveMASH()
	pm := cs.ProposedMASH()
	//fmt.Println("Current mash:\n", cm.String())
	g := cm.Gap()
	if all && g == 0 {
		clusterstate = "Healthy"
	} else {
		clusterstate = "Degraded"
	}
	gapdesc := "contiguous"

	if g != 0 {
		gapdesc = fmt.Sprintf("%.2f%% unmapped", float64(g*100)/float64(HASHRANGE_END))
	}
	rv := fmt.Sprintf("CLUSTER STATE e%d\n  LEADER: %s @%d\n  MASH: %d (%d members, %s)\n  STATE: %s\n", cs.Revision, ldr, cs.LeaderRevision, activeNum, cm.Len(), gapdesc, clusterstate)
	maxnodename := 8
	allnodenames := []string{}
	for _, m := range cs.Members {
		allnodenames = append(allnodenames, m.Nodename)
	}
	sort.Strings(allnodenames)
	for _, m := range cs.Members {
		if len(m.Nodename) > maxnodename {
			maxnodename = len(m.Nodename)
		}
	}
	rv += fmt.Sprintf("  %-"+strconv.Itoa(maxnodename+1)+"s STATUS       MASH   WEIGHT RPREF\n", "NODENAME")
	for _, nn := range allnodenames {
		m := cs.Members[nn]
		var status string
		if m.Active == 0 {
			status += "down+"
		} else {
			status += "up+"
		}
		if !m.IsIn() {
			status += "out"
		} else {
			status += "in"
		}
		if !m.Enabled {
			status += "+dis"
		}
		rv += fmt.Sprintf("  %-"+strconv.Itoa(maxnodename+1)+"s %-12s %-6d %-6d %-5.2f\n", m.Nodename, status, m.Active, m.Weight, m.ReadWeight)
	}
	rv += "ACTIVE " + cm.String() + "\n"
	if activeNum != proposedNum {
		rv += "PROPOSED " + pm.String() + "\n"
	} else {
		rv += "NO PROPOSED MASH\n"
	}
	return rv
}
func (c *etcdconfig) WatchMASHChange(w func(flushComplete chan bool)) {
	c.mu.Lock()
	c.watchers = append(c.watchers, w)
	c.mu.Unlock()
}
func (c *etcdconfig) Fault(fz string, args ...interface{}) {
	/*	if c != nil && c.Faulted() {
		fmt.Printf("ignoring refault, we are already faulted")
	}*/
	reason := fmt.Sprintf(fz, args...)
	trc := string(debug.Stack())
	if c != nil {
		c.faulted = true
		c.eclient.Revoke(c.ctx, c.aliveLeaseID)
		c.ctxCancel()
		fmt.Printf("NODE %s FAULTING\n reason: %s\n trace:\n%s\n", c.nodename, reason, trc)
		panic("Node fault")
	} else {
		fmt.Printf("FAULT WITH NIL PTR\n reason: %s\n trace:\n%s\n", reason, trc)
		panic("Node fault")
	}
}

func (c *etcdconfig) cmanloop() error {
	c.nodehash = murmur.Murmur3([]byte(c.nodename))
	c.ctx, c.ctxCancel = context.WithCancel(context.Background())
	//query initial values
	//set state
	//watch for changes
	//on any change query values and set state
	lresp, err := c.eclient.Grant(c.ctx, 5)
	if err != nil {
		//This is before our alive lease, so do not fault
		panic(err)
	}
	c.aliveLeaseID = lresp.ID
	ch, _ := c.eclient.KeepAlive(c.ctx, c.aliveLeaseID)

	go func() {
		for _ = range ch {
		}
	}()

	err = c.setInitialActive()
	if err != nil {
		return err
	}
	c.setDefaultNodeKeys()
	state := c.queryClusterState()
	c.stateChanged(state)
	go func() {
		eventChan := c.eclient.Watch(c.ctx, c.ClusterPrefix()+"/x", client.WithPrefix(), client.WithRev(state.Revision))
		for ev := range eventChan {
			if ev.Err() != nil {
				c.Fault("watcher got error: %v", err)
			}
			//We don't parse the events directly, but we probably could
			state := c.queryClusterState()
			if state != nil {
				c.stateChanged(state)
			}
		}
		//This can happen if we are faulted
		if c.ctx.Err() == nil {
			c.Fault("Watched died but we are not faulted")
		}
	}()
	return nil
}

// This is just for the first time, it's overly finicky about
// protecting against races
func (c *etcdconfig) setInitialActive() error {
	//check if someone else is alive with out nodename
	alivekey := fmt.Sprintf("%s/x/m/%s/active", c.ClusterPrefix(), c.nodename)
	aliveval := "0"
	var curRev int64

	//We are booting up and establishing ownership of our node name
	//We don't actually know any revisions yet
	resp, err := c.eclient.Get(c.ctx, alivekey)
	if err != nil {
		c.Fault("getting alive: %v", err)
	}
	if resp.Count != 0 {
		return fmt.Errorf("Node exists with same name '%s'", c.nodename)
	}
	curRev = resp.Header.Revision

	txresp, err := c.eclient.Txn(c.ctx).
		If(client.Compare(client.ModRevision(alivekey), "<", curRev)).
		Then(client.OpPut(alivekey, aliveval, client.WithLease(c.aliveLeaseID))).
		Else().
		Commit()
	if err != nil {
		c.Fault("setting alive: %v", err)
	}
	if !txresp.Succeeded {
		return fmt.Errorf("Node exists with same name '%s'", c.nodename)
	}
	return nil
}

func (c *etcdconfig) setDefaultNodeKeys() {
	// /x/m/nodename/enabled
	setifnotexists := func(key, value string) {
		realkey := fmt.Sprintf("%s/x/m/%s/%s", c.ClusterPrefix(), c.nodename, key)
		_, err := c.eclient.Txn(c.ctx).
			If(client.Compare(client.Version(realkey), "=", 0)).
			Then(client.OpPut(realkey, value)).
			Commit()
		if err != nil {
			c.Fault("setting default keys: %v", err)
		}
	}
	setifnotexists("in", True)
	setifnotexists("weight", "100")
	setifnotexists("readweight", "1.0")
	setifnotexists("enabled", True)

	//Also, if there is no mash at all, create an empty mash 1
	resp, err := c.eclient.Get(c.ctx, fmt.Sprintf("%s/x/mash", c.ClusterPrefix()), client.WithPrefix())
	if err != nil {
		c.Fault("getting mash: %v", err)
	}
	if resp.Count == 0 {
		c.trace("publishing default mash")
		_, err := c.eclient.Put(c.ctx, fmt.Sprintf("%s/x/mash/1", c.ClusterPrefix()), "--")
		if err != nil {
			c.Fault("publishing default mash: %v", err)
		}
	}
}
func QueryClusterState(ctx context.Context, cl *client.Client, pfx string) (*ClusterState, error) {
	resp, err := cl.Get(ctx, pfx+"/x", client.WithPrefix(), client.WithSort(client.SortByKey, client.SortAscend))
	if err != nil {
		return nil, err
	}
	rv := &ClusterState{
		Members: make(map[string]*Member),
	}
	rv.Revision = resp.Header.Revision
	mashes := make(map[int64]map[string]*MashRange)
	_ = mashes
	for _, k := range resp.Kvs {
		sk := strings.Split(string(k.Key), "/")
		//expecting prefix/x/m/nodename/?
		//or        prefix/x/mash/<num>/<nodes>/range
		//or        prefix/x/mash/leader
		if len(sk) < 3 {
			fmt.Println("skipping key", string(k.Key))
			continue
		}
		switch sk[2] {
		case "m":
			if len(sk) != 5 {
				fmt.Println("skipping key", string(k.Key))
				continue
			}
			nodename := sk[3]
			mbr, ok := rv.Members[nodename]
			if !ok {
				mbr = &Member{Nodename: nodename}
				rv.Members[nodename] = mbr
				mbr.Hash = murmur.Murmur3([]byte(nodename))
			}
			keyname := sk[4]
			switch keyname {
			case "enabled":
				mbr.Enabled = string(k.Value) == True
			case "in":
				mbr.In = string(k.Value) == True
			case "weight":
				mbr.Weight, err = strconv.ParseInt(string(k.Value), 10, 64)
				if err != nil {
					return nil, fmt.Errorf("bad member weight: %v", err)
				}
			case "readweight":
				mbr.ReadWeight, err = strconv.ParseFloat(string(k.Value), 64)
				if err != nil {
					return nil, fmt.Errorf("bad member readweight: %v", err)
				}
			case "active":
				mbr.Active, err = strconv.ParseInt(string(k.Value), 10, 64)
				if err != nil {
					return nil, fmt.Errorf("bad member active: %v", err)
				}
			default:
				continue
			}
		case "mash":
			if len(sk) == 4 && sk[3] == "leader" {
				rv.Leader = string(k.Value)
				rv.LeaderRevision = k.ModRevision
				continue
			}
			if len(sk) == 6 {

				mashnumber, err := strconv.ParseInt(sk[3], 10, 64)
				if err != nil {
					return nil, fmt.Errorf("bad mash number: %v", err)
				}
				nodename := sk[4]
				if sk[5] != "range" {
					fmt.Println("skipping key", string(k.Key))
					continue
				}
				exmash, ok := mashes[mashnumber]
				if !ok {
					exmash = make(map[string]*MashRange)
					mashes[mashnumber] = exmash
				}
				valsplit := strings.Split(string(k.Value), ",")
				start, err := strconv.ParseInt(valsplit[0], 10, 64)
				if err != nil {
					return nil, fmt.Errorf("bad range: %v", err)
				}
				end, err := strconv.ParseInt(valsplit[1], 10, 64)
				if err != nil {
					return nil, fmt.Errorf("bad range: %v", err)
				}
				exmash[nodename] = &MashRange{Start: start, End: end}
				//fmt.Printf("added %d %s %d .. %d\n", mashnumber, nodename, start, end)
			} else if len(sk) == 4 {
				//Mash leader tag
				mashnumber, err := strconv.ParseInt(sk[3], 10, 64)
				if err != nil {
					return nil, fmt.Errorf("bad mash number: %v", err)
				}
				_, ok := mashes[mashnumber]
				if !ok {
					exmash := make(map[string]*MashRange)
					mashes[mashnumber] = exmash
				}
			} else {
				fmt.Println("huh", sk)
			}
		default:
			fmt.Println("skipping key", string(k.Key))
			continue
		}
	}
	rv.Mashes = mashes
	//fmt.Printf("we read mashes as %+v\n", mashes)
	return rv, nil
}
func (c *etcdconfig) updateNotifiedCache() {
	pm := c.cachedState.MashAt(c.notifiedMashNum)
	var ourNotifiedStart int64 //inclusive
	var ourNotifiedEnd int64   //exclusive
	for idx, hash := range pm.Hashes {
		if hash == c.nodehash {
			ourNotifiedStart = pm.Ranges[idx].Start
			ourNotifiedEnd = pm.Ranges[idx].End
			break
		}
	}
	c.notifiedRangeMu.Lock()
	c.ourNotifiedStart = ourNotifiedStart
	c.ourNotifiedEnd = ourNotifiedEnd
	c.notifiedRangeMu.Unlock()
}
func (c *etcdconfig) queryClusterState() *ClusterState {
	cs, err := QueryClusterState(c.ctx, c.eclient, c.ClusterPrefix())
	if err != nil {
		c.Fault("Querying cluster state: %v", err)
		return nil
	}
	//Get member advertised endpoints
	for nodename, member := range cs.Members {
		grpce, err := c.PeerGRPCAdvertise(nodename)
		if err == nil {
			member.AdvertisedEndpointsGRPC = grpce
		} else {
			//I wonder where this might happen?
			panic(err)
		}
		httpe, err := c.PeerHTTPAdvertise(nodename)
		if err == nil {
			member.AdvertisedEndpointsHTTP = httpe
		} else {
			panic(err)
		}
	}
	cs.c = c
	return cs
}

//Highest map num, all at max
func (s *ClusterState) ProposedMashNumber() (proposed int64, active int64, allmax bool) {
	var max int64
	for k, _ := range s.Mashes {
		if k > max {
			max = k
		}
	}
	active = 0
	allsame := true
	count := 0
	for _, m := range s.Members {
		if m.IsIn() {
			if m.Active != max {
				allsame = false
			}
			if active == 0 || m.Active < active {
				active = m.Active
			}
			count += 1
		}
	}
	//Although they are all technically the same, you can't make a new mash
	//because you need to wait for them to be at the current one
	if count == 0 {
		allsame = false
	}
	return max, active, allsame
}
func (s *ClusterState) IdealLeader() uint32 {
	var max uint32
	for _, m := range s.Members {
		if m.IsIn() {
			h := murmur.Murmur3([]byte(m.Nodename))
			if h > max {
				max = h
			}
		}
	}
	return max
}
func (s *ClusterState) HasLeader() bool {
	return s.Leader != ""
}
func (c *etcdconfig) stateChanged(s *ClusterState) {
	c.cachedStateMu.Lock()
	c.cachedState = s
	c.cachedStateMu.Unlock()
	fmt.Printf("State changed: \n%s", s)

	if !s.Members[c.nodename].Enabled {
		c.Fault("node disabled")
		return
	}
	if s.Members[c.nodename].Active == 0 && c.weExpectToBeUp {
		c.Fault("we are not active but we expect to be")
		return
	}
	if s.Members[c.nodename].Active != 0 && s.Members[c.nodename].IsIn() {
		c.weExpectToBeUp = true
	}
	// What is the current mash map
	// What is our mash map? If there is a change to our map, notify
	// once notifications are done, advance our mash map
	// Who is the leader? If it is us, work out if there needs
	// to be a change in mash map
	proposedMash, _, allcurrent := s.ProposedMashNumber()
	if allcurrent { //This would include us then too
		c.trace("allcurrent = true")
		//No proposed mash, perhaps we need a leader?
		if !s.HasLeader() || s.Leader == c.nodename || (s.IdealLeader() == c.nodehash && s.Members[c.nodename].IsIn()) {
			//We should be the leader
			c.trace("we want to be leader")
			c.doLeaderStuff(s)
		} else {
			c.trace("we do not want to be leader")
		}
	} else {
		c.trace("allcurrent is false")
	}
	c.trace("proposedMash is %d, our target mash is %d", proposedMash, c.notifiedMashNum)
	if c.notifiedMashNum < proposedMash {
		c.trace("we want to advance our mash to %d", proposedMash)
		c.notifiedMashNum = proposedMash
		c.updateNotifiedCache()
		notifydone := make([]chan bool, len(c.watchers))
		for i, f := range c.watchers {
			f(notifydone[i])
		}
		go func() {
			for _, ch := range notifydone {
				<-ch
			}
			//This might be really after we notified
			//and the ClusterState may have changed several times
			//but we won't have done any other notifies because
			// a) notifiedHashNum equals proposedMash
			// b) proposedMash can't change until we set our active to proposedMash
			alivekey := fmt.Sprintf("%s/x/m/%s/active", c.ClusterPrefix(), c.nodename)
			aliveval := strconv.FormatInt(proposedMash, 10)
			_, err := c.eclient.Put(c.ctx, alivekey, aliveval, client.WithLease(c.aliveLeaseID))
			if err != nil {
				c.Fault("putting alive key: %v", err)
				return
			}
		}()
	} else {
		c.trace("we do not want to advance our mash")
	}

}
func (mm *MASHMap) Len() int {
	return len(mm.Hashes)
}
func (mm *MASHMap) Swap(i, j int) {
	mm.Hashes[i], mm.Hashes[j] = mm.Hashes[j], mm.Hashes[i]
	mm.Nodenames[i], mm.Nodenames[j] = mm.Nodenames[j], mm.Nodenames[i]
	mm.Weights[i], mm.Weights[j] = mm.Weights[j], mm.Weights[i]
	if len(mm.Ranges) != 0 {
		mm.Ranges[i], mm.Ranges[j] = mm.Ranges[j], mm.Ranges[i]
	}
}
func (mm *MASHMap) Less(i, j int) bool {
	return mm.Hashes[i] < mm.Hashes[j]
}
func (mm *MASHMap) String() string {
	rv := "MASH\n"
	for i := 0; i < mm.Len(); i++ {
		rv += fmt.Sprintf("[%08x] %12s : %d - %d (%d)\n", mm.Hashes[i], mm.Nodenames[i], mm.Ranges[i].Start, mm.Ranges[i].End, mm.Weights[i])
	}
	return rv
}
func (s *ClusterState) ProposedMASH() *MASHMap {
	rv := &MASHMap{c: s.c}
	proposed, _, _ := s.ProposedMashNumber()
	i := 0
	for nodename, nrange := range s.Mashes[proposed] {
		rv.Hashes = append(rv.Hashes, murmur.Murmur3([]byte(nodename)))
		rv.Nodenames = append(rv.Nodenames, nodename)
		nrc := *nrange
		rv.TotalWeight += s.Members[nodename].Weight
		rv.Ranges = append(rv.Ranges, &nrc)
		rv.Weights = append(rv.Weights, s.Members[nodename].Weight)
		i++
	}
	sort.Sort(rv)
	return rv
}

func (s *ClusterState) ActiveMASH() *MASHMap {
	rv := &MASHMap{c: s.c}
	_, active, _ := s.ProposedMashNumber()
	i := 0
	for nodename, nrange := range s.Mashes[active] {
		rv.Hashes = append(rv.Hashes, murmur.Murmur3([]byte(nodename)))
		rv.Nodenames = append(rv.Nodenames, nodename)
		nrc := *nrange
		rv.TotalWeight += s.Members[nodename].Weight
		rv.Ranges = append(rv.Ranges, &nrc)
		rv.Weights = append(rv.Weights, s.Members[nodename].Weight)
		i++
	}
	sort.Sort(rv)
	return rv
}
func (s *ClusterState) MashAt(v int64) *MASHMap {
	rv := &MASHMap{c: s.c}
	i := 0
	for nodename, nrange := range s.Mashes[v] {
		rv.Hashes = append(rv.Hashes, murmur.Murmur3([]byte(nodename)))
		rv.Nodenames = append(rv.Nodenames, nodename)
		nrc := *nrange
		rv.TotalWeight += s.Members[nodename].Weight
		rv.Ranges = append(rv.Ranges, &nrc)
		rv.Weights = append(rv.Weights, s.Members[nodename].Weight)
		i++
	}
	sort.Sort(rv)
	return rv
}
func (s *ClusterState) IdealMash() *MASHMap {
	rv := &MASHMap{c: s.c}
	for _, m := range s.Members {
		if !m.IsIn() {
			continue
		}
		rv.TotalWeight += m.Weight
		rv.Hashes = append(rv.Hashes, murmur.Murmur3([]byte(m.Nodename)))
		rv.Nodenames = append(rv.Nodenames, m.Nodename)
		rv.Weights = append(rv.Weights, m.Weight)
	}
	sort.Sort(rv)
	var loc int64
	for i := 0; i < rv.Len(); i++ {
		// Note, weights must be uint32s so this is ok
		share := (rv.Weights[i] * (1 << 32)) / rv.TotalWeight
		rv.Ranges = append(rv.Ranges, &MashRange{Start: loc, End: loc + share})
		loc += share
		//Compensate for any little errors. We need to completely cover the uint32 space
		if i == rv.Len()-1 {
			rv.Ranges[i].End = (1 << 32)
		}
	}
	return rv
}

//Return the range that is the intersection of mashRange and the free space in
//mm if you remove 'excluding' (the hash of a node)
func (mm *MASHMap) IntersectWithFreeSpace(r *MashRange, excluding uint32) *MashRange {
	rv := &MashRange{Start: r.Start, End: r.End}
	if rv.Start == rv.End {
		//this is a zero length slice, it doesn't intersect
		return rv
	}
	for i := 0; i < mm.Len(); i++ {
		if mm.Hashes[i] == excluding {
			continue
		}
		//Ranges with zero length are nbd
		if mm.Ranges[i].Start == mm.Ranges[i].End {
			continue
		}
		//Trim rv so it does not intersect with Ranges[i]
		//case 1 no intersection at all, ri fully < rv or ri fully > rv
		if mm.Ranges[i].End <= rv.Start || rv.End <= mm.Ranges[i].Start {
			continue
		}
		//case 2: ri overlaps with the left of rv or the whole of rv
		if mm.Ranges[i].End > rv.Start && mm.Ranges[i].Start <= rv.Start {
			rv.Start = mm.Ranges[i].End
			if rv.Start >= rv.End {
				//We are going to be zerolen for now
				rv.Start = r.Start
				rv.End = rv.Start
				return rv
			}
			continue
		}
		//case 3: ri is inside rv
		if mm.Ranges[i].Start >= rv.Start && mm.Ranges[i].End <= rv.End {
			//Pick the largest bit left
			if mm.Ranges[i].Start-rv.Start > rv.End-mm.Ranges[i].End {
				//Biggest part is on the left
				rv.End = mm.Ranges[i].Start
			} else {
				//Biggest part is on the right
				rv.Start = mm.Ranges[i].End
			}
			if rv.Start >= rv.End {
				//We are going to be zerolen for now
				rv.Start = r.Start
				rv.End = rv.Start
				return rv
			}
			continue
		}
		//case 4: ri overlaps with the right of rv
		if mm.Ranges[i].Start < rv.End && mm.Ranges[i].End >= rv.End {
			rv.End = mm.Ranges[i].Start
			if rv.Start >= rv.End {
				//We are going to be zerolen for now
				rv.Start = r.Start
				rv.End = rv.Start
				return rv
			}
			continue
		}
		//Sanity check (expanded or unnoticed zero length (we want zl to be at r.start))
		if rv.Start < r.Start || rv.End > r.End || rv.Start >= rv.End {
			mm.c.Fault("sanity check failed %#v %#v", rv, r)
			return nil
		}
	}
	return rv
}
func (mm *MASHMap) Gap() int64 {
	var gap int64
	var lastend int64
	for i := 0; i < mm.Len(); i++ {
		if mm.Ranges[i].Start == mm.Ranges[i].End {
			//not a real range
			continue
		}
		gap += mm.Ranges[i].Start - lastend
		if mm.Ranges[i].Start < lastend {
			mm.c.Fault("sanity check AA failed")
			return -1
		}
		if i+1 != mm.Len() {
			if mm.Ranges[i].End > mm.Ranges[i+1].Start &&
				mm.Ranges[i+1].Start != mm.Ranges[i+1].End {
				mm.c.Fault("sanity check AB failed: %v %v", mm.Ranges[i], mm.Ranges[i+1])
				return -1
			}
		} else {
			if mm.Ranges[i].End > HASHRANGE_END {
				mm.c.Fault("sanity check AC failed")
				return -1
			}
		}
		lastend = mm.Ranges[i].End
	}
	gap += HASHRANGE_END - lastend
	return gap
}

//Return true if these maps are identical
func (mm *MASHMap) Equivalent(rhs *MASHMap) bool {
	if mm.Len() != rhs.Len() {
		return false
	}
	for i := 0; i < mm.Len(); i++ {
		if mm.Nodenames[i] != rhs.Nodenames[i] ||
			mm.Ranges[i].Start != rhs.Ranges[i].Start ||
			mm.Ranges[i].End != rhs.Ranges[i].End {
			return false
		}
	}
	return true
}
func (currentMash *MASHMap) CompatibleIntermediateMash(idealMash *MASHMap) *MASHMap {
	//The rule is that any node's start and end can be extended or shrunk, but
	//only so far as they do not collide with the original ranges of other nodes

	rv := &MASHMap{c: currentMash.c}
	//First, the members of the new mash come from the ideal mash
	//So do the weights (they are just for reference)
	for i := 0; i < idealMash.Len(); i++ {
		rv.Hashes = append(rv.Hashes, idealMash.Hashes[i])
		rv.Nodenames = append(rv.Nodenames, idealMash.Nodenames[i])
		rv.Weights = append(rv.Weights, idealMash.Weights[i])
	}
	//The algorithm here is to find the empty space each node has
	//(which might be everything if there were no nodes) and
	//then get the intersection of that and the ideal mash range for
	//the same node. Because it was free space, it won't collide, and
	//because it is the intersection with ideal, it gets us closer to
	//the goal
	rv.TotalWeight = idealMash.TotalWeight
	for i := 0; i < rv.Len(); i++ {
		rv.Ranges = append(rv.Ranges, currentMash.IntersectWithFreeSpace(idealMash.Ranges[i], idealMash.Hashes[i]))
	}

	//Sanity check, nothing in the returned map must overlap with itself or
	//with a range not itself in currentMash
	for i := 0; i < rv.Len(); i++ {
		if rv.Ranges[i].Start == rv.Ranges[i].End {
			//zerolen range doesn't matter
			continue
		}
		for j := 0; j < currentMash.Len(); j++ {
			if !(rv.Ranges[i].Start >= currentMash.Ranges[j].End ||
				rv.Ranges[i].End <= currentMash.Ranges[j].Start ||
				rv.Hashes[i] == currentMash.Hashes[j] ||
				currentMash.Ranges[j].Start == currentMash.Ranges[j].End) {
				currentMash.c.Fault("sanity check BA failed %+v/%s vs %+v/%s", rv.Ranges[i], rv.Nodenames[i], currentMash.Ranges[j], currentMash.Nodenames[j])
				return nil
			}
		}
	}
	fmt.Println("GAP IS ", rv.Gap())
	return rv
}
func (s *ClusterState) ActiveMembers() int {
	rv := 0
	for _, m := range s.Members {
		if m.IsIn() {
			rv += 1
		}
	}
	return rv
}

//Called if everyone is on the same version, and we are
//the ideal leader
func (c *etcdconfig) doLeaderStuff(s *ClusterState) {
	leaderkey := fmt.Sprintf("%s/x/mash/leader", c.ClusterPrefix())
	if s.Leader != c.nodename {
		//Set ourselves as leader
		resp, err := c.eclient.Txn(c.ctx).
			If(client.Compare(client.ModRevision(leaderkey), "=", s.LeaderRevision)).
			Then(client.OpPut(leaderkey, c.nodename, client.WithLease(c.aliveLeaseID))).
			Commit()
		if err != nil {
			c.Fault("could not write to leader: %v", err)
			return
		}
		if !resp.Succeeded {
			//Someone else wrote their key. This will trigger an event and cause us to
			//reevaluate leader stuff if we were the rightful heir anyway
			c.trace("we failed to assume leadership")
		} else {
			c.trace("leadership written")
		}
	} else {
		c.trace("we are already leader")
	}
	// Only do leadery stuff if there are active members
	if s.ActiveMembers() == 0 {
		c.trace("Not doing leadery stuff, no active members")
		return
	}
	//From this point, do leader stuff atomically only if leaderkey is us.
	proposedMashNum, activeMashNum, allcurrent := s.ProposedMashNumber()
	c.trace("current mash number is %d", activeMashNum)
	currentMash := s.ProposedMASH()
	//Verify again there is no new mash
	_, ok := s.Mashes[activeMashNum+1]
	if ok || !allcurrent || proposedMashNum != activeMashNum {
		c.Fault("sanity check CA failed")
		return
	}
	idealMash := s.IdealMash()
	if currentMash.Equivalent(idealMash) {
		c.trace("no changes to mash required")
		return
	}
	nextMash := currentMash.CompatibleIntermediateMash(idealMash)
	nextMashNum := activeMashNum + 1
	fmt.Printf("Proposing new mash %d\n%s\n", nextMashNum, nextMash.String())
	fmt.Printf("The ideal mash is\n%s\n==\n", idealMash.String())
	var opz []client.Op
	for i := 0; i < nextMash.Len(); i++ {
		v := fmt.Sprintf("%d,%d", nextMash.Ranges[i].Start, nextMash.Ranges[i].End)
		opz = append(opz, client.OpPut(fmt.Sprintf("%s/x/mash/%d/%s/range", c.ClusterPrefix(), nextMashNum, nextMash.Nodenames[i]), v))
	}
	opz = append(opz, client.OpPut(fmt.Sprintf("%s/x/mash/%d", c.ClusterPrefix(), nextMashNum), c.nodename))
	resp, err := c.eclient.Txn(c.ctx).
		If(client.Compare(client.Value(leaderkey), "=", c.nodename)).
		Then(opz...).
		Commit()
	if err != nil {
		c.Fault("could not write mash %v", err)
		return
	}
	if !resp.Succeeded {
		fmt.Println("huh, we lost leadership just as we were about to do good in the world")
		//not actually a problem. We'll get here again if we need to
	} else {
		c.trace("we did leadership")
	}
}
