QUASAR
======

The QUery Augmented Stratified ARchiver is a new experimental archiver in
development to support high density data storage applications.

### Installation

To run an archiver, make sure that you have Go >= 1.3 installed and then
run the following:

```
go get github.com/SoftwareDefinedBuildings/quasar/qserver
```

If you want to use the quasar tool (used for inspecting databases and
reaping old generations), then do the following:

```
go get github.com/SoftwareDefinedBuildings/quasar/qtool
```

Both of these sets of commands will install the tools into your
$GOPATH/bin directory. If you have this directory on your $PATH then you do
not need to do anything further. Otherwise you will need to add the binaries
to your $PATH variable manually. 

As an alternative to using the binaries in the $GOPATH, you can generate
a binary in the current directory by typing

```
go build github.com/SoftwareDefinedBuildings/quasar/qserver
```

### Making a database

As a recent addition, quasar no longer requires a preallocated database. 
Do not underestimate how fast it fills up the database, however, especially if it is not
regularly reaped (reaping is not currently automatic). The data is designed to be 
very compressible at a filesystem level, so the recommended deployment is on top
of a ZFS dataset with compress=lz4.

The following command builds a 64GB database in /srv/quasar/

```
qserver -makedb 64
```

If you wish to override the directory that the database is stored, you
can pass -dbpath to the makedb command and the normal serving command:

```
qserver -makedb 64 -dbpath /home/oski/quasar/
```

Note that in addition to the database, there is a virtual address table 
that uses 2GB of memory and disk space for every TB of database space. 
Technically this can be paged out, so that you can run it on a PC with 
insufficient memory but this harms performance. 

### Running

The default configuration for the server is to listen for cpnp traffic
on localhost:4410. This means that the server cannot be used as an http
archiver, and only programs on the local computer can communicate with it.
To remove this restriction, use the cpnp and http flags:

```
qserver -cpnp :4410 -http :9000
```

In addition to the communication flags, there is a most recently used cache
that can be configured. This defaults to 2GB, but should be increased if
RAM permits. Put together, a good set of arguments for serving a database out
of a home directory is:

```
qserver -cache 32 -cpnp :4410 -http :9000 -dbpath /home/oski/quasar
```

### Reaping and inspecting

The majority of quasar data is snapshots of the database as it was in the past.
Until an automatic reaper is made, this must be manually done while the
database is stopped:

```
qtool reap /path/to/database uuid1 uuid2 uuid3 ...
```

This deletes all trees before the latest generation of the given uuids.

If the database is shut down before a transaction is comitted, there are also
a number of allocated but unwritten "leaked" blocks. These can be freed with

```
qtool freeleaks /path/to/database
```

You can see the number of allocated, freed and leaked blocks with

```
qtool inspect /path/to/database
```
 






