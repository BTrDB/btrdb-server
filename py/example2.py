
import quasar
from twisted.internet import defer, reactor

ud="418f1b6d-5836-4589-ad52-0ecfd8d82059"

def e(args):
	print "error: ", args

def rv((stat, arg)):
	print "stat: ", stat
	print "arg: ", arg

def qsvrv((stat, arg)):
	if stat == "ok":
		ver, vals = arg
		for i in vals:
			print i.time, i.value
			print type(i.time), type(i.value)
def onConnect(q):
	print "Connected to archiver"
	d = q.insertValues(ud, [(i, i*10) for i in xrange(10000)])
	d.addCallback(rv)
	d.addErrback(e)
	d = q.queryStandardValues(ud, 0, 200)
	d.addCallback(qsvrv)
	d.addErrback(e)

d = quasar.connectToArchiver("localhost")
d.addCallback(onConnect)
d.addErrback(e)

reactor.run()