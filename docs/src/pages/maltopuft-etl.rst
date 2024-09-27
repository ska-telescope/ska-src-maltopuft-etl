=============
MALTOPUFT ETL
=============

The MALTOPUFT ETL package contains a series of scripts designed to extract data archives from various sources and load them into a MALTOPUFT DB instance. The normalised MALTOPUFT DB schema has been designed to satisfy the requirements of the MALTOPUFT application for candidate labelling and curating Machine Learning training/evaluation datasets. 

The intended data sources include SKA precursor and pathfinder telescopes, such as MeerTRAP and LOFAR, in addition to existing known pulsar and fast-transient catalogues, such as ATNF and TNS.

Ideally, the SKA will create new observation metadata and candidate records in MALTOPUFT DB with (near-) real time API, preventing the need for ETL scripts to process SKA archive data.

Sources
=======

Supported and planned sources are listed below:

.. csv-table:: :rst:dir:`Supported sources`
   :header: "Source", "Supported"

   "MeerTRAP", "✅"
   "LOFAR", "❌"
   "ATNF", "✅"
   "TNS", "❌"
