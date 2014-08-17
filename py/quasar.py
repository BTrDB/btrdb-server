import capnp
import os
import os.path
import socket
import struct
import uuid

from twisted.internet import defer, protocol, reactor
from twisted.internet.protocol import Factory, Protocol
from twisted.internet.endpoints import TCP4ClientEndpoint, connectProtocol

scriptpath = os.path.dirname(os.path.realpath(__file__))
quasar_cpnp = capnp.load(os.path.join(scriptpath, "quasar.cpnp"))

def connectToArchiver(addr, port=4410):
        ep = TCP4ClientEndpoint(reactor, addr, port)
        d = connectProtocol(ep, Quasar())
        return d

class QuasarFactory(Factory):
    def buildProtocol(self, addr):
        return Quasar()

class Quasar(Protocol):

    LATEST = 0

    def connectionMade(self):
        self.defmap = {}
        self.etag = 1
        self.have = ""
        self.expecting = 0

    def _normalizeUUID(self, u):
        if len(u) == 36:
            #probably a string
            tmp = uuid.UUID(u)
            return tmp.bytes
        elif len(u) == 16:
            #probably already bytes
            return u
        else:
            raise Exception("Malformed UUID: %s", u)

    def _newmessage(self):
        msg = quasar_cpnp.Request.new_message()
        msg.echoTag = self.etag
        rdef = defer.Deferred()
        self.defmap[self.etag] = rdef
        self.etag += 1
        return msg, rdef

    def _txmessage(self, msg):
        packet = msg.to_bytes()
        self.transport.write(packet)

    def _processSegment(self, data):
        resp = quasar_cpnp.Response.from_bytes(data)
        if not resp.echoTag in self.defmap:
            print "[E] bad echo tag"
            return
        rdef = self.defmap[resp.echoTag]

        if resp.which() == 'records':
            recs = resp.records
            rdef.callback((resp.statusCode, (recs.version, recs.values)))
        elif resp.which() == 'statisticalRecords':
            recs = resp.statisticalRecords
            rdef.callback((resp.statusCode, (recs.version, recs.values)))
        elif resp.which() == 'versionList':
            rv = {}
            for idx, uid in enumerate(resp.versionList.uuids):
                rv[uid] = resp.versionList.versions[idx]
            rdef.callback((resp.statusCode, (rv,)))
        elif resp.which() == 'changedRngList':
            rdef.callback((resp.statusCode, (resp.changedRngList.values,)))
        elif resp.which() == 'void':
            rdef.callback((resp.statusCode, ()))
        else:
            print "No idea what type of response this is: ", resp.which()
            raise Exception("bad response")

    def dataReceived(self, data):
        self.have += data
        print "rx: %d bytes ", len(data)
        if self.expecting == 0 and len(self.have) >= 8:
            AB, C, D = struct.unpack("<IHH", self.have[:8])
            print "rx seg AB=%d C=%d D=%d" % (AB, C, D)
            self.expecting = 8+C*8+D*8
            if D !=0 or AB != 0:
                raise Exception("Interesting header")
        if len(self.have) >= self.expecting:
            self._processSegment(self.have[:self.expecting])
            self.have = self.have[self.expecting:]
            self.expecting = 0

    def queryStandardValues(self, uid, start, end, version=LATEST):
        msg, rdef = self._newmessage()

        qsv = msg.init('queryStandardValues')
        qsv.uuid=self._normalizeUUID(uid)
        qsv.version=version
        qsv.startTime=start
        qsv.endTime=end

        self._txmessage(msg)
        return rdef

    def queryStatisticalValues(self, uid, start, end, rfactor, version=LATEST):
        msg, rdef = self._newmessage()

        if rfactor < 0 or rfactor > 61:
            raise ValueError("Invalid reduction factor")

        qsv = msg.init('queryStatisticalValues')
        qsv.uuid=self._normalizeUUID(uid)
        qsv.version=version
        qsv.startTime=start
        qsv.endTime=end
        qsv.pointWidth=rfactor

        self._txmessage(msg)
        return rdef

    def insertValues(self, uid, records, sync=False):
        msg, rdef = self._newmessage()

        iv = msg.init('insertValues')
        iv.uuid = self._normalizeUUID(uid)
        iv.sync = sync
        recs = iv.init('values', len(records))
        for k in xrange(len(records)):
            recs[k].time = records[k][0]
            recs[k].value = records[k][1]

        self._txmessage(msg)
        return rdef

    def deleteRange(self, uid, start, end):
        msg, rdef = self._newmessage()

        dr = msg.init('deleteValues')
        dr.uuid = self._normalizeUUID(uid)
        dr.start = int(start)
        dr.end = int(end)

        self._txmessage(msg)
        return rdef

    def queryChangedRanges(self, uid, fromgen, togen, threshold=0):
        msg, rdef = self._newmessage()

        qcr = msg.init('queryChangedRanges')
        qcr.uuid = self._normalizeUUID(uid)
        qcr.fromGeneration = fromgen
        qcr.toGeneration = togen
        qcr.threshold = threshold

        self._txmessage(msg)
        return rdef

    def queryNearestValue(self, uid, time, version=LATEST, backward=True):
        msg, rdef = self._newmessage()

        qn = msg.init('queryNearestValue')
        qn.uuid = self._normalizeUUID(uid)
        qn.time = int(time)
        qn.backward = bool(backward)
        qn.version = version

        self._txmessage(msg)
        return rdef

    def queryVersion(self, uids):
        msg, rdef = self._newmessage()

        qv = msg.init('queryVersion')
        uidlist = qv.init('uuids', len(uids))
        for i in xrange(len(uids)):
            uidlist[i] = self._normalizeUUID(uids[i])

        self._txmessage(msg)
        return rdef




