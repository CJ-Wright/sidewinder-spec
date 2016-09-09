# sidewinder-spec
Side loader of legacy/SPEC data and images into the NSLS-II stack

## NSLS-II stack
The [NSLS-II](https://github.com/nsls-ii) stack consists of the following projects:
- [metadatastore](https://github.com/nsls-ii/metadatastore) which holds 
experiment metadata in a mongodb
- [filestore](https://github.com/nsls-ii/filestore) which holds the paths for 
large files (e.g. images) and has a handler system to load the data in a 
flexable and agnostic way
- [databroker](https://github.com/nsls-ii/databroker) which provides a convient 
way to view and query the data in from the previous two databases

## Utilities beyond the NSLS-II stack
This project aims to provide some utilities for parsing:
- QXRD generated tiff.metadata files into python dictionaries
- SPEC file parsing not provided by [IXStools](www.github
.com/NSLS-II-IXS/ixstools) which may be a bit beamline specific

## Requirements
The NSLS-II stack
- metadatastore
- filestore
- databroker
