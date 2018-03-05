.. SHEDsidewinder documentation master file, created by
   sphinx-quickstart on Wed Feb 28 12:46:40 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to SHEDsidewinder's documentation!
===========================================

SHEDsidewinder is a project for side loading data from synchrotron and other
beam studies into a databroker.

Quickstart
==========

New databroker
--------------

If you don't have a databroker already (or want to create a new one) run:

.. code-block:: sh

   sidewind init <db_name> <db_path>

where ``<db_name>`` is what the databroker will be called, and ``<db_path>``
is where the data in the database will be stored

Add data to existing databroker
-------------------------------

If you have a databroker and want to add data to it run:

.. code-block:: sh

   sidewind <facility_name> <path_to_data>

where ``<facility_name>`` is the facility name (check which are available
with ``sidewind -h`` and ``<path_to_data>`` is the path to the folder or
file containing the data.

.. toctree::
   :maxdepth: 4
   :caption: Contents:

   shed_sidewinder


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
