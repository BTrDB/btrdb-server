QUASAR
======

The QUery Augmented Stratified ARchiver is a high performance time series
database designed to support high density data storage applications.

### Dependencies

Quasar uses a MongoDB collection to store metadata. Also, if installed in High Availability
mode, it requires a ceph pool. Note that even if not using ceph, librados needs to be 
installed.

### Installation

To run an archiver, make sure that you have Go >= 1.4 installed and then
run the following:

```
apt-get install librados-dev
go get github.com/SoftwareDefinedBuildings/quasar/qserver
```

This will install the tools into your
$GOPATH/bin directory. If you have this directory on your $PATH then you do
not need to do anything further. Otherwise you will need to add the binaries
to your $PATH variable manually. 

Note that in order to run the quasar server, you will need to copy quasar.conf
from the github repository to /etc/quasar/quasar.conf (or the directory that
you are in).

An alternative to 'go get'ing to your GOPATH is to clone the repository then do:

```
apt-get install librados-dev
go get -d ./... && go install ./qserver
```

This will also put the qserver binary in your $GOPATH/bin.

### Configuration

Sensible defaults (for a production deployment) are already found in quasar.conf. Some things you may need
to adjust:
 - The MongoDB server and collection name
 - The block cache size (defaults to 32GB). Note that quasar uses more than this, this is just
   a primary contributor to the RAM footprint.
 - The file storage path or ceph details

Once your configuration is set up, you can set up the files, and database indices with

```
qserver -makedb
```

Which should print out:
```
Configuration OK!
Creating a new database
Done
```

You can now run a server with:
```
qserver
```




