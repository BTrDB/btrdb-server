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

The main distribution of BTrDB v4 is smartgridstore which is basically BTrDB packaged for deployment on Kubernetes, along with some (optional) utilities for working with synchrophasors. You can follow the installation guide on https://docs.smartgrid.store

If you are interested in deploying BTrDB, please file an issue on this repository so we can get an idea for what type of deployments we should focus on supporting. Smartgrid.store is focused on very large city or country-scale data aggregation from smart grid devices, but BTrDB can support other use cases. 

