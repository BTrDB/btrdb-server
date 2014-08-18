from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from SocketServer import ThreadingMixIn
import json
import os
import pymongo
import requests
import string
import sys
import urllib

client = pymongo.MongoClient()
mongo_collection = client.qdf.metadata

class HTTPRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write('GET request received')
        
    def do_POST(self):
        self.query = self.rfile.read(int(self.headers['Content-Length']))
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET POST')
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        if self.query == 'select distinct Metadata/SourceName':
            sources = set()
            for stream in mongo_collection.find():
                sources.add(stream['Metadata']['SourceName'])
            self.wfile.write(json.dumps(list(sources)))
        elif self.query.startswith('select distinct Path where Metadata/SourceName'):
            source = self.query.split('"')[1]
            paths = set()
            for stream in mongo_collection.find({"$where": 'this.Metadata.SourceName === "{0}"'.format(source)}):
                paths.add(stream['Path'])
            self.wfile.write(json.dumps(list(paths)))
        elif self.query.startswith('select * where Metadata/SourceName'):
            parts = self.query.split('"')
            source = parts[1]
            path = parts[3]
            streams = set()
            for stream in mongo_collection.find({"$where": 'this.Metadata.SourceName === "{0}" && this.Path === "{1}"'.format(source, path)}):
                del stream['_id']
                streams.add(json.dumps(stream))
            returnstr = '['
            for stream in streams:
                returnstr += stream + ", "
            self.wfile.write(returnstr[:-2] + ']')
        elif self.query.startswith('select * where uuid ='): # I assume that it's a sequence of ORs
            parts = self.query.split('"')
            uuids = []
            i = 0
            while i < len(parts):
                if i % 2 != 0:
                    uuids.append('this.uuid === "{0}"'.format(parts[i]))
                i += 1
            streams = set()
            for stream in mongo_collection.find({"$where": ' || '.join(uuids)}):
                del stream['_id']
                streams.add(json.dumps(stream))
            returnstr = '['
            for stream in streams:
                returnstr += stream + ", "
            self.wfile.write(returnstr[:-2] + ']')
        else:
            self.wfile.write('[]')
                    
class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    pass
        
serv = ThreadedHTTPServer(('', 4523), HTTPRequestHandler)
serv.serve_forever()
