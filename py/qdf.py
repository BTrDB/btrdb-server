__author__ = 'immesys'

import isodate
import datetime
import time
from pymongo import MongoClient
import os
import uuid
import quasar
import signal

from twisted.internet import defer, protocol, reactor



_client = MongoClient(os.environ["QUASAR_MDB_HOST"])

OPTIMAL_BATCH_SIZE = 100000
def onFail(param):
    print "Encountered error: ", param

def register(instantiation, options=None):
    instantiation.mdb = _client.qdf
    instantiation.setup(options)
    instantiation.sync_metadata()
    d = quasar.connectToArchiver(os.environ["QUASAR_ARCHIVER_HOST"], int(os.environ["QUASAR_ARCHIVER_PORT"]))
    def onConnect(q):
        print "CONNECTED TO ARCHIVER"
        instantiation._db = q
        instantiation._cycle()

    d.addCallback(onConnect)
    d.addErrback(onFail)
    #Push metadata here

def begin():
    def reregisterctrlc():
        pass #signal.signal(signal.SIGINT, signal.default_int_handler)
    reactor.callLater(0, reregisterctrlc)
    reactor.run()

class QuasarDistillate(object):
    def __init__(self):
        self._version = None
        self._author = None
        self._name = None
        self._streams = {}
        self._consumed = {}
        self._deps = {}
        self._metadata = {}
        self._db = None
        self._old_streams = []

    @staticmethod
    def date(dst):
        """
        This parses a modified isodate into nanoseconds since the epoch. The date format is:
        YYYY-MM-DDTHH:MM:SS.NNNNNNNNN
        Fields may be omitted right to left, but do not elide leading or trailing zeroes in any field
        """
        idate = dst[:26]
        secs = (isodate.parse_datetime(idate) - datetime.datetime(1970,1,1)).total_seconds()
        nsecs = int(secs*1000000) * 1000
        nanoS = dst[26:]
        if len(nanoS) != 3 and len(nanoS) != 0:
            raise Exception("Invalid date string!")
        if len(nanoS) == 3:
            nsecs += int(nanoS)
        return nsecs

    @defer.inlineCallbacks
    def _cycle(self):
        yield self._nuke_old_streams()
        print "Invoking computation"
        then = time.time()
        yield self.compute()
        for skey in self._streams:
            self.stream_flush(skey)
        print "Computation done (%.3f ms)" % ((time.time() - then)*1000)

    @staticmethod
    def now():
        nsecs = int(time.time()*1000000)*1000
        return nsecs

    def set_version(self, ver):
        self._version = ver

    def set_author(self, author):
        self._author = author

    def set_name(self, name):
        self._name = name

    def add_stream(self, name, unit):
        self._streams[name] = {"name" : name, "unit": unit, "store":[]}

    def set_metadata(self, key, value):
        self._metadata[key] = value

    def persist(self, name, value):
        self.mdb.persistence.update({"author":self._author, "name":self._name, "version":self._version, "fieldname":name},
                                    {"author":self._author, "name":self._name, "version":self._version, "fieldname":name,
                                     "value":value},upsert=True)

    @defer.inlineCallbacks
    def stream_get(self, name, start, end):
        if name not in self._consumed:
            raise ValueError("Input stream '%s' not added in config()" % name)
        rv = yield self._db.queryStandardValues(self._consumed[name]["uuid"], start, end)
        statcode, (version, values) = rv
        if statcode != "ok":
            raise ValueError("Bad request")
        defer.returnValue((version, values))

    def use_stream(self, name, uid):
        self._consumed[name] = {"name":name, "uuid":uid}

    @defer.inlineCallbacks
    def stream_flush(self, name):
        print "flushing stream"
        if len(self._streams[name]["store"]) == 0:
            return
        uid = self._streams[name]["uuid"]
        d = self._db.insertValues(uid, [(v[0], float(v[1])) for v in self._streams[name]["store"]])
        self._streams[name]["store"] = []
        def rv((stat, arg)):
            print "Insert rv:", stat, arg
        d.addCallback(rv)
        d.addErrback(onFail)
        yield d

    @defer.inlineCallbacks
    def stream_insert(self, name, time, value):
        yield self.stream_insert_multiple(name, [(time, value)])

    @defer.inlineCallbacks
    def stream_insert_multiple(self, name, values):
        if name not in self._streams:
            raise ValueError("Output stream '%s' not added in config()" % name)
        self._streams[name]["store"] += values
        if len(self._streams[name]["store"]) >= 50000:
            yield self.stream_flush(name)

    def unpersist(self, name, default):
        doc = self.mdb.persistence.find_one({"author":self._author, "name":self._name,
                                            "version":self._version, "fieldname":name})
        if doc is None:
            return default
        return doc["value"]

    @defer.inlineCallbacks
    def _nuke_old_streams(self):
        for skey in self._old_streams:
            print "Script version has increased. Nuking existing data for '%s' (this can take a while)" % skey
            rv = yield self._db.deleteRange(self._streams[skey]["uuid"], quasar.MIN_TIME, quasar.MAX_TIME)
            print "rv was: ", rv

    def sync_metadata(self):
        for skey in self._streams:
            path = "/%s/%s/%s" % (self._author, self._name, self._streams[skey]["name"])
            deps = ", ".join("%s" % self._deps[ky] for ky in self._deps)
            doc = self.mdb.metadata.find_one({"Path":path})
            if doc is None:
                uid = str(uuid.uuid4())
                ndoc = {
                    "Path" : path,
                    "Metadata" :
                    {
                        "SourceName" : "Distillate",
                        "Instrument" :
                        {
                            "ModelName" : "Distillate Generator",
                        },
                        "Author": self._author,
                        "Version": self._version,
                        "Name": self._name,
                        "Dependencies": deps,
                    },
                    "uuid" : uid,
                    "Properties" :
                    {
                        "UnitofTime" : "ns",
                        "Timezone" : "UTC",
                        "UnitofMeasure" : self._streams[skey]["unit"],
                        "ReadingType" : "double"
                    }
                }
                self._streams[skey]["uuid"] = uid
                for k in self._metadata:
                    ndoc["Metadata"][k] = self._metadata[k]
                self.mdb.metadata.save(ndoc)
            else:
                if int(doc["Metadata"]["Version"]) < self._version:
                    print "stream mismatch: ", int(doc["Metadata"]["Version"]), self._version
                    self._old_streams.append(skey)

                self._streams[skey]["uuid"] = doc["uuid"]
                sdoc = {"Metadata.Version":self._version, "Metadata.Dependencies": deps}
                for k in self._metadata:
                    sdoc["Metadata"][k] = self._metadata[k]
                self.mdb.metadata.update({"Path":path},{"$set":sdoc},upsert=True)
                print "we inserted new version"

