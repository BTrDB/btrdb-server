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

OPTIMAL_BATCH_SIZE = 100000
MICROSECOND = 1000
MILLISECOND = 1000*MICROSECOND
SECOND      = 1000*MILLISECOND
MINUTE      = 60*SECOND
HOUR        = 60*MINUTE
DAY         = 24*HOUR
MIN_TIME    = -(16<<56)
MAX_TIME    = (48<<56)
LATEST      = 0

_client = MongoClient(os.environ["QUASAR_MDB_HOST"])

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
        instantiation._start()

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
        self._stream_versions = {}

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
        print "Invoking computation"
        then = time.time()
        yield self.compute()
        for skey in self._streams:
            self.stream_flush(skey)
        for useds in self._stream_versions:
            used_st = self._stream_versions[useds]
            for skey in self._streams:
                path = "/%s/%s/%s" % (self._author, self._name, self._streams[skey]["name"])
                self.mdb.metadata.update({"Path":path},
                    {"$set":{"Metadata.DependencyVersions.%s" % used_st["uuid"]: used_st["version"]}})

        print "Computation done (%.3f ms)" % ((time.time() - then)*1000)
        reactor.callLater(5, self._cycle)

    @defer.inlineCallbacks
    def _start(self):
        yield self._nuke_old_streams()
        reactor.callLater(1, self._cycle)

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

    def get_version_of_last_query(self, name):
        if len(self._streams) == 0:
            raise ValueError("Cannot have an input stream with no output")

        if name not in self._consumed:
            raise ValueError("Input stream '%s' not added in config()" % name)
        anykey = self._streams.keys()[0]
        out_uuid = self._streams[anykey]["uuid"]
        in_uuid = self._consumed[name]["uuid"]
        doc = self.mdb.metadata.find_one({"uuid" : out_uuid})
        if doc is None:
            "No such record for stream"
            return None
        try:
            ver = doc["Metadata"]["DependencyVersions"][in_uuid]
            return ver
        except KeyError:
            print "Key error: doc was: ",doc
            return None

    def set_metadata(self, key, value):
        self._metadata[key] = value

    @staticmethod
    def _combine_ranges(ranges):
        combined_ranges = []
        num_streams = len(ranges)
        while True:
            progress = False
            combined = False
            #find minimum range
            minidx = 0
            for idx in xrange(num_streams):
                if len(ranges[idx]) == 0:
                    continue
                progress = True
                if ranges[idx][0][0] < ranges[minidx][0][0]:
                    minidx = idx
            #Now see if any of the other ranges starts lie before it's end
            for idx in xrange(num_streams):
                if len(ranges[idx]) == 0:
                    continue
                if idx == minidx:
                    continue
                if ranges[idx][0][0] <= ranges[minidx][0][1]:
                    if ranges[idx][0][1] > ranges[minidx][0][1]:
                        ranges[minidx][0][1] = ranges[idx][0][1]
                    ranges[idx] = ranges[idx][1:]
                    combined = True
            if not progress:
                break
            if not combined:
                combined_ranges.append(ranges[minidx][0])
                ranges[minidx] = ranges[minidx][1:]
        return combined_ranges

    @defer.inlineCallbacks
    def stream_delete_range(self, name, start, end):
        if name not in self._streams:
            raise ValueError("Output stream '%s' not added in config()" % name)

        statcode, rv = yield self._db.deleteRange(self._streams[name]["uuid"], start, end)
        if statcode != "ok":
            raise Exception("Bad delete")

    @defer.inlineCallbacks
    def get_changed_ranges(self, names, gens):
        if gens == "auto":
            gens = ["auto" for i in names]

        uids = []
        for n in names:
            if n not in self._consumed:
                print self._consumed
                raise ValueError("Input stream '%s' not added in config()" % name)
            uids.append(self._consumed[n]["uuid"])

        fgens = []
        for gidx in xrange(len(names)):
            if gens[gidx] == "auto":
                ver = self.get_version_of_last_query(names[gidx])
                print "defaulting to version 1"
                if ver is None:
                    ver = 1
            else:
                ver = gens[gidx]
            fgens.append(ver)

        ranges = [[] for x in uids]
        for idx in xrange(len(uids)):
            print "invoking qcr with",uids[idx], fgens[idx]
            statcode, rv = yield self._db.queryChangedRanges(uids[idx], fgens[idx], LATEST, 16000)
            print statcode, rv
            if statcode != "ok":
                raise Exception("Bad range query")
            ranges[idx] = [[v.startTime, v.endTime] for v in rv[0]]

        combined_ranges = self._combine_ranges(ranges)
        defer.returnValue(combined_ranges)

    def persist(self, name, value):
        self.mdb.persistence.update({"author":self._author, "name":self._name, "version":self._version, "fieldname":name},
                                    {"author":self._author, "name":self._name, "version":self._version, "fieldname":name,
                                     "value":value},upsert=True)

    @defer.inlineCallbacks
    def stream_get(self, name, start, end):
        if name not in self._consumed:
            raise ValueError("Input stream '%s' not added in config()" % name)
        uid = self._consumed[name]["uuid"]
        rv = yield self._db.queryStandardValues(uid, start, end)
        statcode, (version, values) = rv
        self._stream_versions[name] = {"uuid":uid, "version":version}
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
            rv = yield self._db.deleteRange(self._streams[skey]["uuid"], MIN_TIME, MAX_TIME)
            print "rv was: ", rv

    def sync_metadata(self):
        for skey in self._streams:
            path = "/%s/%s/%s" % (self._author, self._name, self._streams[skey]["name"])
            deps = ", ".join("%s" % self._deps[ky] for ky in self._deps)
            doc = self.mdb.metadata.find_one({"Path":path})
            if doc is not None and doc["Metadata"]["Version"] != self._version:
                print "Rewriting metadata: version bump"
                self.mdb.remove({"Path":path})
                doc = None
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

