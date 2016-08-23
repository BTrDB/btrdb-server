BTrDB
=====

The Berkeley TRee DataBase is a high performance time series
database designed to support high density data storage applications.

We are now doing binary and container releases with (mostly) standard semantic versioning.
The only variation on this is that we will use odd-numbered minor version numbers to indicate
and unstable/development release series. Therefore the meanings of the version numbers are:
 - Major: an increase in major version number indicates that there is no backwards
   compatibility with existing databases. To upgrade, we recommend using the migration
   tool.
 - Minor: minor versions are compatible on-disk, but may have an incompatible network API. Therefore
   while it is safe to upgrade to a new minor version number, you may need to upgrade other
   programs that connect to BTrDB too. Furthermore, odd-numbered minor version numbers should
   be considered unstable and for development use only, patch releases within an odd numbered
   minor version number may not be compatible with eachother.
 - Patch: patch releases on an odd numbered minor version number are not necessarily compatible
   with eachother in any way. Patch releases on an even minor version number are guaranteed to
   be compatible both in the disk format and in network API.

While using odd-numbered versions to indicate development releases is a somewhat archaic practice, it allows us to use our production release system for development, which reduces the odds that there is a discrepancy between the well-tested development binaries/containers and the subsequently released production version. Note that we will flag all development releases as "pre-release" on github.

### Releases

Binary releases are available on github, and container releases are available on Docker hub as btrdb/release:<version>. Note that we publish a given version, eg x.y.z as release:latest, release:x, release:x.y and release:x.y.z . We recommend that you reference the btrdb/release:x.y container and periodically pull the image to allow for automatic patch release upgrades.

### Dependencies

BTrDB uses a MongoDB collection to store metadata. Also, if installed in High Availability
mode, it requires a ceph pool. Note that even if not using ceph, librados needs to be
installed.

### Rapid installation with docker

If you want to try out BTrDB with zero hassles, you can simply use our docker image.

```
# a docker network helps by allowing DNS resolution between containers
docker network create mynet
docker run -d --net mynet --name mongo mongo:3.2
docker run -it --net mynet -v /my/datadir:/srv -e BTRDB_MONGO_SERVER=mongo.mynet btrdb/release:3.4 makedb
docker run -d -v /my/datadir:/srv -p 4410:4410 btrdb/release:3.4
```

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
