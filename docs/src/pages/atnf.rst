ATNF
====

The `Australia Telescope National Facility (ATNF) pulsar catalogue <https://www.atnf.csiro.au/people/pulsar/psrcat/>`_ catalogues many known pulsars. Known pulsars are used in MALTOPUFT to understand if a detected candidate's fitted dispersion measure in a paticular patch of sky could have come from an existing known source.

The ATNF ETL script aims to extract the parameters relevant to MALTOPUFT from the ATNF catalogue and store a static copy of these in MALTOPUFT DB.

.. note::

    MALTOPUFT DB stores *only the required known pulsar parameters to facilitate candidate labelling with MALTOPUFT*. It is not, nor will ever, be intended to replace the ATNF catalogue.

    The key reasons for storing a local copy of this data is to reduce latency when plotting known sources, and ensure that MALTOPUFT does not exert unexpected load on the ATNF servers.

Data source
-----------

The ATNF pulsar catalogue is available to `download <https://www.atnf.csiro.au/people/pulsar/psrcat/download.html>`_ and the available parameters are documented in the `ATNF pulsar catalogue documentation <https://www.atnf.csiro.au/people/pulsar/psrcat/psrcat_help.html>`_.

Schemas
-------

Per the `ATNF documentation <https://www.atnf.csiro.au/people/pulsar/psrcat/psrcat_help.html>`_, the attributes extracted and used by MALTOPUFT are:

.. csv-table:: :rst:dir:`ATNF attributes used in MALTOPUFT`
   :header: "Source", "Description", "Units"

    "Name", "Pulsar name", "The B name if exists, otherwise the J name"
    "RAJ", "Right ascension (J2000)", "hours, minutes, seconds (hh:mm:ss.s)"
    "DecJ", "Declination (J2000)", "degrees, mintues, seconds (+dd:mm:ss) "
    "DM", "Dispersion measure. The integrated free electron density along the line of sight between the pulse and the observer.", "cm-3 pc"
    "W50", "Width of pulse at 50% of peak", "ms"
    "P0", "Barycentric period of the pulsar", "s"

Extraction
----------

The ATNF ETL scripts use the `psrqpy <https://psrqpy.readthedocs.io/en/latest/>`_ python package to extract the most recent ATNF pulsar catalogue tarball and include the parameters relevant to MALTOPUFT in a Pandas DataFrame.

.. note::

    The psrqpy package caches a local version of the catalogue tarball so that repeated downloads are not required.
    
    When running the ATNF script in Docker, the filesystem is currently ephemeral. This means that the catalogue caching is not being used effectively, and the tarball is downloaded on every run.

    MALTOPUFT DB stores a list of accessed catalogues and their visit history. In the future, the ETL scripts will determine whether the ATNF catalogue, or any future external catalogue, needs revisiting by looking up records in this table.

Transformations
---------------

After the parameters are extracted, they are appended with the ``known_ps.`` key. The name of the catalogue and information such as the visit URL and visit time are stored and prefixed with the ``cat.`` key.
