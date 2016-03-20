# sidewinder-spec
Translator from APS 11-ID-B SPEC data and images into the NSLS-II stack

## NSLS-II stack
The [NSLS-II](www.github.com/NSLS-II) stack consists of the following projects:
- [metadatastore](www.github.com/NSLS-II/metadatastore) which holds 
experiment metadata in a mongodb
- [filestore](www.github.com/NSLS-II/filestore) which holds the paths for 
large files (e.g. images) and has a handler system to load the data in a 
flexable and agnostic way
- [databroker](www.github.com/NSLS-II/databroker) which provides a convient 
way to load the data in from the previous two databases

## Utilities beyond the NSLS-II stack
This project aims to provide some utilities for parsing:
- QXRD generated tiff.metadata files into python dictionaries
- SPEC file parsing not provided by [IXStools](www.github
.com/NSLS-II-IXS/ixstools) which may be a bit beamline specific
- Parsing of the temperature controler output
We will also provide a complete pipeline, provided the data/metadata is 
loaded into the NSLS-II stack, to get from experimental tiff files with 
calibration parameters, a background tiff/chi file, potentially a starting 
mask and some PDF parameters to G(r) via PDFgetx3

## Requirements
The NSLS-II stack (obviously)
- metadatastore
- filestore
- databroker
- pyFAI for image integration/calibration
- IXStools (maybe if it provides valuble SPEC parsing)
- PDFgetx3 for performing the I(Q)->G(r) conversion, I haven't decided if I 
am going to try to make a nice function inside of the python pipeline or if I
 need to use subprocess