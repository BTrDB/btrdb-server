
import quasar
from twisted.internet import defer, reactor

ud="3e7b8bd7-0a12-43ed-a035-901d5789b3de"

def e(args):
    print "error: ", args

def rv((stat, arg)):
    print "stat: ", stat
    print "arg: ", arg

def qsvrv((stat, arg)):
    if stat == "ok":
        ver, vals = arg
        print "got back", len(vals)
        #for i in vals:
        #	print i.time, i.value
        #	print type(i.time), type(i.value)
def onConnect(q):
    print "Connected to archiver"
    day = 1000000000*60*60*24
    d = q.queryStandardValues(ud, 1408336102847398000-day, 1408336102847398000)
    d.addCallback(qsvrv)
    d.addErrback(e)

d = quasar.connectToArchiver("localhost")
d.addCallback(onConnect)
d.addErrback(e)

reactor.run()