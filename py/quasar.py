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

MIN_TIME = -(16<<56)
MAX_TIME = (48<<56)

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
        self.hdrexpecting = 0
        self.gothdr = False
        self.numsegs = 0
        self.hdrptr = 0

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
        resp = quasar_cpnp.Response.from_bytes(data, traversal_limit_in_words = 100000000, nesting_limit=1000)
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
        if self.expecting == 0:
            if self.hdrexpecting == 0 and len(self.have) >= 4:
                #Move to the first stage of decoding: work out header length
                self.numsegs, = struct.unpack("<I", self.have[:4])
                self.numsegs += 1
                self.hdrexpecting = (self.numsegs) * 4
                if self.hdrexpecting % 8 == 0:
                    self.hdrexpecting += 4
                self.hdrptr = 4
            if self.hdrexpecting != 0 and len(self.have) >= self.hdrexpecting + self.hdrptr:
                for i in xrange(self.numsegs):
                    segsize, = struct.unpack("<I", self.have[self.hdrptr:self.hdrptr+4])
                    self.expecting += segsize*8
                    self.hdrptr += 4

        if len(self.have) >= self.expecting + self.hdrexpecting + 4:
            ptr = self.expecting + self.hdrexpecting + 4
            self._processSegment(self.have[:ptr])
            self.have = self.have[ptr:]
            self.expecting = 0
            self.numsegs = 0
            self.hdrexpecting = 0
        #print "rx: %d bytes have/expecting", len(data), len(self.have), self.expecting

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
        dr.startTime = int(start)
        dr.endTime = int(end)

        self._txmessage(msg)
        return rdef

    def queryChangedRanges(self, uid, fromgen, togen=LATEST, threshold=0):
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




