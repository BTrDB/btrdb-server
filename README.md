BTrDB
=====

The Berkeley TRee DataBase is a high performance time series
database designed to support high density data storage applications.
This project used to be called QUASAR, but we have changed the name
partly to match publications, and partly as a flag day. The capnp interface
in BTrDB is designed to better support large queries and clusters and is not 
backwards compatible with the quasar interface.

### Dependencies

BTrDB uses a MongoDB collection to store metadata. Also, if installed in High Availability
mode, it requires a ceph pool. Note that even if not using ceph, librados needs to be 
installed.

### Installation

To run an archiver, make sure that you have Go >= 1.4 installed and then
run the following:

```
apt-get install librados-dev
go get github.com/SoftwareDefinedBuildings/btrdb/btrdbd
```

This will install the tools into your
$GOPATH/bin directory. If you have this directory on your $PATH then you do
not need to do anything further. Otherwise you will need to add the binaries
to your $PATH variable manually. 

Note that in order to run the btrdb server, you will need to copy btrdb.conf
from the github repository to /etc/btrdb/btrdb.conf (or the directory that
you are in).

An alternative to 'go get'ing to your GOPATH is to clone the repository then do:

```
apt-get install librados-dev
go get -d ./... && go install ./btrdbd
```

This will also put the btrdbd binary in your $GOPATH/bin.

### Configuration

Sensible defaults (for a production deployment) are already found in btrdb.conf. Some things you may need
to adjust:
 - The MongoDB server and collection name
 - The block cache size (defaults to 32GB). Note that quasar uses more than this, this is just
   a primary contributor to the RAM footprint.
 - The file storage path or ceph details

Once your configuration is set up, you can set up the files, and database indices with

```
btrdbd -makedb
```

Which should print out:
```
Configuration OK!
Creating a new database
Done
```

You can now run a server with:
```
btrdbd
```

### Using the database

Note that we are presently working on release engineering, and hope to release the first (public) version in August 2016. If you are using it now, bear in mind it is still in development.

To communicate with the database, there are [go bindings](https://github.com/SoftwareDefinedBuildings/btrdb-go) and [python bindings](https://github.com/SoftwareDefinedBuildings/btrdb-python). The go bindings are faster and more maintained.



