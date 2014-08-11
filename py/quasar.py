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

	def queryStandardValues(self, uuid, start, end, version=LATEST):
		msg = quasar_cpnp.Request.new_message()
		msg.echoTag = self.etag
		rdef = defer.Deferred()
		self.defmap[self.etag] = rdef
		self.etag += 1

		qsv = msg.init('queryStandardValues')
		qsv.uuid=self._normalizeUUID(uuid)
		qsv.version=version
		qsv.startTime=start
		qsv.endTime=end
        packet = msg.to_bytes()
        self.transport.write(packet)
		return rdef

	def insertValues(self, uuid, records):
		msg = quasar_cpnp.Request.new_message()
		msg.echoTag = self.etag
		rdef = defer.Deferred()
		self.defmap[self.etag] = rdef
		self.etag += 1

		iv = msg.init('insertValues')
		iv.uuid = self._normalizeUUID(uuid)
		recs = iv.init('values', len(records))
		for k in xrange(len(records)):
			recs[k].time = records[k][0]
			recs[k].value = records[k][1]

        packet = msg.to_bytes()
        self.transport.write(packet)
		return rdef


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
			for idx, uuid in enumerate(resp.versionList.uuids):
				rv[uuid] = resp.versionList.versions[idx]
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

