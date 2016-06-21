# sidewinder-spec
Translator from APS 11-ID-B SPEC data and images into the NSLS-II stack

## NSLS-II stack
The [NSLS-II](https://github.com/nsls-ii) stack consists of the following projects:
- [metadatastore](https://github.com/nsls-ii/metadatastore) which holds 
experiment metadata in a mongodb
- [filestore](https://github.com/nsls-ii/filestore) which holds the paths for 
large files (e.g. images) and has a handler system to load the data in a 
flexable and agnostic way
- [databroker](https://github.com/nsls-ii/databroker) which provides a convient 
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
- PDFgetx3 for performing the I(Q)->G(r) conversion, We haven't decided if We 
am going to try to make a nice function inside of the python pipeline or if We
 need to use subprocess
 
## Installation
We have written an installation script for installing this and the rest of the [analysis stack](https://gist.github.com/CJ-Wright/105a311f69ce2a32116f45163cfcd2af), note that this is a work in progress.
If however, you wish to install this by itself, just clone it and run `python setup.py install`.

## Usage
1. Write the configuration files. Each directory which holds the data to be added to the database needs to have a `config.txt` file. This file will hold some basic information on the experiment ran. An example configuration file can be found under the examples folder. At a bare minimum 2 things are needed, the name of the experiment ran and the calibration file folder. Note that any additional information provided under other headers will be added to the run headers as additional information
2. Run `pyFAI-calib` on the calibration data. All calibration images must be used to produce `.poni` files *before* any data is loaded into the DB. This may change with the advent of analysisstore but currently the loaders look for `.poni` files upon loading calibration files.
3. Run `loader.load_beamtime('/path/to/file', 'path/to/spec_file', dry_run=False)`. This will load all the folder which have `config.txt` files into the DB, ready for use.
4. Explore your data, we highly recommend using [xpd_workflow](https://github.com/CJ-Wright/xpd_workflow) and [scikit-beam](https://github.com/scikit-beam/scikit-beam)