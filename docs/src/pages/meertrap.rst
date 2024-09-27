MeerTRAP
========

The MeerTRAP ETL scripts load existing file-based MeerTRAP data archives in to the MALTOPUFT DB schema.

Data source
-----------

MeerTRAP single pulse candidate data consists of a series of files.

All files related to a single candidate are stored under a common directory on a filesystem. The directory is named with the hostname of the MeerTRAP server responsible for processing the beam in which the candidate was detected and the Unix timestamp when the detection took place.

The candidate directory records candidate fit data in a tab-separated *SPCCL* file. Observatory level metadata is stored in a *run summary* JSON file. Candidate diagnostic subplots, including a frequency vs. time plot for the detected pulse, are stored in a JPEG file.

The files are stored and named in the following directory structure:

.. code:: bash

    data/
    ├─ <hostname>_<unix_ts>/
    │  ├─ <datetime>_beam<absolute_beam_number>.spccl.log
    │  ├─ <datetime>_<hostname>_run_summary.json
    │  ├─ <mjd>_DM_<dm>_beam_<absolute_beam_number><beam_coherence>.jpg
    ├─ .../

.. note::

    Currently, MeerTRAP data is stored on MeerTRAP servers and periodically distributed to other server locations such as Jodrell Bank Center for Astrophysics.
    
    To use the prototype ETL scripts, it is required that you:
    
    1. Have permission to access data on the filesystem of a server where MeerTRAP data is hosted. If you are unsure if you have sufficient access to a server, please contact a project maintainer.
    2. Store data with the directory structure outlined above on a filesystem in the same network as the ETL process. For example, if the ETL scripts are deployed with docker compose then you should ensure that the data volume is mounted in the container. For an example, please refer to the `example docker compose <https://gitlab.com/ska-telescope/src/maltopuft/ska-src-maltopuft-backend/-/blob/03784ec12c0f7439c9903ad93ebd691a583475e2/docker/test.docker-compose.yaml#L67-68>`_.

The MeerTRAP ETL script parses each candidate's data and applies the necessary transformations to align the data with the target MALTOPUFT DB schema. Finally, the data is inserted into MALTOPUFT DB.

The absolute path to the ``data/`` directory holding candidate is configured with the ``data_path`` parameter in ``src/config.yml``.

Schemas
-------

-----
SPCCL
-----

.. csv-table:: :rst:dir:`SPCCL schema`
   :header: "Attribute name", "Data type", "Description", "Notes",

    "index", "int",	"Candidate primary key.", "In practice, each SPCCL file contains one candidate, so this is always 0.",
    "mjd", "float",	"Time of pulse observation in Modified Julian Date time units.", "",
    "dm", "float", "Dispersion measure value fitted in the candidate search pipeline.", "",	
    "width", "float", "Pulse full-width half maximum (FWHM).", "",	
    "snr", "float",	"Pulse signal to noise ratio.", "",
    "beam", "int", "Absolute beam number in the observation.", "",
    "beam_mode", "char", "( C | I ) for coherent/incoherent beam.", "Introduced in SPCCL v2.",
    "ra", "str", "Right ascension", "In units of hours, minutes, seconds (hms).",
    "dec", "str", "Declination", "In units of degrees, minutes, seconds (dms).",
    "label", "int", "Human assigned label. ( 0 | 1 ) for RFI label, single pulse label.", "Introduced in SPCCL v3.",
    "probability", "float", "Single pulse MeerTRAP ML classification probability.", "Introduced in SPCCL v3.",
    "fil_file", "str", "Filterbank file path.", "",
    "plot_file", "str", "Diagnostic sub-plot file path.", "",


Extraction
----------

-----------
Run summary
-----------

Run summary files are serialised to JSON. The JSON object is then parsed with a Pydantic model which validates the attributes.

Some minor cleaning to make the data easier to work with is also applied during Pydantic parsing. For example, if an attribute contains a JSON string (``"attribute":"{\"sub_attribute\":\"my_value\",}""``) then this could be further serialised to JSON and parsed by another Pydantic model.

Cleaning such as this is done on a case-by-case basis. For example, if the JSON string has a predictable/uniform schema then this approach would be viable. However, JSON strings which have unknown numbers of attributes or non-uniform schemas will remain as a string. During transformations, these strings are instead 'exploded' into new rows with references to their related data.

This decision is made to ensure that Pydantic models can be dumped into a normalised DataFrame with a fixed width/number of columns.

.. warning::

    Many candidates will have duplicate run summary files. This means there is currently a lot of unneccessary duplicated file parsing in the Meerkat ETL script.

    One improved approach could be to first compute the hash of each run summary file in the candidate dataset. Duplicates can therefore be removed before parsing and transformation stage.

    One caveat with this approach is that some run summary files are *almost* identical. For example, there can be cases where a run summary file with the same name can exist with an absent ``utc_stop`` attribute or with a valid ``utc_stop`` attribute. In such cases, the run summary with a valid ``utc_stop`` should be parsed, as this attribute gives the observation end time.

-----
SPCCL
-----

Lines in SPCCL files are extracted and then serialised to JSON. Similar to above, the JSON object is parsed and validated with Pydantic models and dumped to a normalised DataFrame.

Transformations
---------------

After extraction, DataFrames are transformed into the target schema. This step includes adding the Foreign Keys to related database tables in the target schema.

Transformed columns that match the target schema are prefixed with a key (for example, ``key.``) to indicate which table in the target schema they belong to. This assists with loading the data into the database.

--------------------
Observation metadata
--------------------

Observation metadata is parsed from the run summary JSON file. The observation metadata transformations are outlined below.

==============
Schedule block
==============

A schedule block refers to a logically grouped series of calibration and target observations. The schedule block start time is parsed from the ``sb_details.actual_start_time`` attribute:

.. code:: javascript

    {
        "sb_details": {
            ...
            "actual_start_time": "2023-11-20 21:37:42.000Z", 
            ...
        }
    }

.. warning::
    
    In general, the ``sb_details.actual_start_time`` and ``sb_details.scheduled_time`` attributes are not equal due to scheduling latency at the observatory.

If available, the schedule block end time is parsed from the ``sb_details.actual_end_time`` attribute.

.. code:: javascript

    {
        "sb_details": {
            ...
            "actual_end_time": null, 
            ...
        }
    }

This attribute is often ``null``. In this case, the following strategy is employed to estimate the schedule block end time.

If the ``sb_details.expected_duration_seconds`` is not ``null``, then this attribute is added on the ``actual_start_time``.

.. warning::
    This approach doesn't result in an accurate end time due to the fact that the individual observations in the run summary may overrun or experience scheduling latencies. Therefore a more accurate way to determine this from the run summary information will be required in the future.

If the ``sb_details.expected_duration_seconds`` is ``null``, then this value can be calculated by summing the track duration of the individual observation targets that make up the schedule block. In the example below, the ``expected_duration_seconds`` attribute is ``0 + 600 = 600``, resulting in an estimated schedule block end time of ``2023-11-20 21:47:42``.

.. code:: javascript

    "sb_details": {
        "targets": [
            {
                "track_start_offset":32.66579222679138,
                "target":"J0408-6545",
                "track_duration":0.0,
            },
            {
                "track_start_offset":33.66579222679138,
                "target":"J0408-6545",
                "track_duration":600.0,
            }
        ],
    }

.. warning::

    It has been noted that the ``sb_details.targets`` attribute is ``null`` values in some cases. However, this information is populated by parsing arguments from the scripts responsible for configuring and running the schedule block at the observatory.

    In practice, this means that parsing ``sb_details.script_profile_config`` is a more robust way to retreive the schedule block targets than ``sb_details.targets``. This method is therefore used in the data transformation scripts.

Columns containing schedule block attributes are prefixed with the ``sb.`` key.

======================
Meerkat schedule block
======================

There is a 1:1 correspondence between a schedule block and a Meerkat schedule block. These attributes are separated such that Meerkat specific information (such as proposal IDs, run IDs, etc.) are not stored in the same table as the core schedule block information. Instead, each row in the schedule block table has a foreign key to the MeerKAT-specific schedule block information. This means that schedule blocks are a generic entity that can be used not only for MeerTRAP, but other SKA precursors and the SKA. The Meerkat attributes are extracted from the following run summary attributes:

.. code:: javascript

    {
        "sb_details": {
            "id": 79119,
            "id_code": "*****",
            "proposal_id": "*****",
        }
    }

Columns containing Meerkat schedule block attributes are prefixed with the ``mk_sb.`` key.

===========
Observation
===========

An observation is a continuous period of time where one target is being observed, potentially for calibration.

.. note::

    The observation data model intends to be compatible with the `IVOA ObsCore <https://wiki.ivoa.net/internal/IVOA/ObsCoreDMvOnedotOne/WD-ObsCore-v1.1-20160127.pdf>`_ data model, with the intent that MALTOPUFT will provide a `TAP service <https://wiki.ivoa.net/twiki/bin/view/IVOA/TableAccess>`_ in the fuure.

    At the time of writing, a formal extension for the ObsCore model for time-domain radio astronomy does not exist. The attributes used in the data model are primarily based on the time-domain radio astronomy extension proposed and currently in use by `The National Institute for Astrophsics (INAF) <https://wiki.ivoa.net/internal/IVOA/InterOpApr2022TDIG-Radio-CSP/ObsCore_mapping_for_INAF_pulsar_FRB_data.pdf>`_.

    This means that this data model may be subject to change as a standard is converged on.

.. warning::

    An individual run summary file is related to the observation of a single target. This means that all candidates detected while observing the same target will have (almost) identical run summary contents.

    However, this means that, if an entire observation passes without detecting a candidate, then **a run summary will not exist for this observation** in the candidate detection data set.

    This means that the current approach does not guarantee a full record of every observation carried out during a schedule block.

The observation start time is parsed from the ``utc_start`` attribute and transformed into the standard ISO 8601 datetime format:

.. code:: javascript

    {
        "utc_start": "2023-11-20_21:57:11",
    }

The observation end time is given by the ``utc_stop`` attribute. However, this is often ``null``. As discussed above, observation scheduling latency also means that this cannot be accurately inferred by summing the ``track_duration`` of each observation in the schedule block.

Therefore the following strategy is used to infer the observation end time.

1. If ``utc_stop`` is not null, then use the given value.
2. If ``utc_stop`` is null, then use the minimum value of the schedule block end time (plus a 1 hour buffer to account for potential accumulated latency in scheduling the schedule block's observations) or the start time (``utc_start``) of the subsequent observation when observations are sorted in ascending time order.

.. warning::

    There are several issues with using this approach to infer the observation end time.

    Firstly, the schedule block end time is inferred based on the expected duration of all observations and this error will be carried forward.

    Secondly, as discussed above, there is no guarantee that a record for every observation exists in the candidate detection dataset. This means that the end time of an observation should be considered an upper bound.

The remaining observation attributes are determined straightforwardly, according to the proposed INAF time-domain radio astronomy ObsCore extension.

===========================
Coherent beam configuration
===========================

These attributes describe properties of an observation's coherent beams. In general, these are computed during observatory scheduling based on information such as the target/source being observed, the number of beams required etc.

The attributes are parsed directly from the ``beams.coherent_beam_shape`` attribute in the run summary JSON:

.. code:: javascript

    {
        "beams": {
            "coherent_beam_shape": {
                "angle": -54.5255677366855, 
                "overlap": 0.25, 
                "x": 0.008135835724328964, 
                "y": 0.0074917003204572116
            },
    }

.. warning::
    
    Although the coherent beam configuration is computed per observation, there can in principle be duplicated configurations depending on the specifics of the observations being carried out.

Columns containing coherent beam config attributes are prefixed with the ``cb.`` key.

====================
Tiling configuration
====================

An observation's beams are tiled to form a coninuous view of a patch (or distinct patches) of sky being observed. Tiling configuration records store information about the number of, shape and amount of overlap between beams, the Unix timestamp where the tiling configuration was applied etc.

The tiling configuration is parsed from the following attributes:

.. code:: javascript

    {
        "beams": {
        "ca_target_request": {
            "tilings": [
                {
                    "coordinate_type": "equatorial",
                    "epoch": 1700517405.395673,
                    "epoch_offset": 300.0,
                    "method": "variable_size",
                    "nbeams": 780,
                    "overlap": 0.25,
                    "reference_frequency": 1284000000.0,
                    "shape": "circle",
                    "shape_parameters": [],
                    "tags": [
                        "noapsuse"
                    ],
                    "target": "J0440-4333, radec gaincal, 4:40:17.07, -43:33:09.0"
                }
            ],
        }
    }

Due to the movement of the Earth while observing a given source, beams may require re-tiling while observing a target. At MeerTRAP, this re-tiling typically occurs every 10 minutes. MALTOPUFT refers to an observation period with a new tiling configuration as an *observation*. In general, there may be several tiling configurations (*observations*) related to one source.

Columns containing tiling attributes are prefixed with the ``tiling.`` key.

====
Beam
====

MeerTRAP processes beams in a distributed nature. Beams are *sharded*, such that one server/host is responsible for processing a unique subset of the total number of observing beams throughout the observation life cycle.

The beams being processed by a given host are parsed from the ``beams.list`` attribute. 

.. code:: javascript

    {
        "beams": {
            "list": [
                {
                    "absnum": 0,
                    "coherent": true,
                    "dec_dms": "-43:33:09.0",
                    "mc_ip": "1.1.1.1",
                    "mc_port": 1234,
                    "ra_hms": "4:40:17.07",
                    "relnum": 0,
                    "source": "J0440-4333"
                }, 
                {
                    "absnum": 1,
                    "coherent": true,
                    "dec_dms": "-43:34:20.7",
                    "mc_ip": "1.1.1.1",
                    "mc_port": 1234,
                    "ra_hms": "4:40:13.06",
                    "relnum": 1,
                    "source": "J0440-4333"
                },
            ]
        },
    }

This records the position of the beam on the sky in right ascension (declination) coordinates in units of hours (degrees), minutes, seconds. The absolute beam number out of all the observation's beams are recorded in the ``beams.list.absnum`` attribute, whereas the ``beams.list.relnum`` records the beam number relative to the host.

There are two key cases while parsing beam information. A host can be responsible for *either* processing coherent or incoherent beams. A coherent beam host is responsible for processing up to 12 coherent beams, whereas an incoherent beam host only proccesses one beam.

.. warning::

    There should be one, and only one, incoherent beam record at any given time.

    By contrast to coherent beams, incoherent beams do not require periodic re-tiling. Therefore, the *same incoherent beam* can persist across several, or even inbetween, observations.

    Despite being the 'same' incoherent beam in principle a new record is made for incoherent beams belonging to different observations.

Columns containing beam attributes are prefixed with the ``beam.`` key.

====
Host
====

As discussed above, several hosts are used to process MeerTRAP beams. Each host records the IP address, hostname and port number of unique hosts.

.. note::

    IP addresses come and go. If nodes are swapped in/out of the cluster then, depending on the circumstances, the IP address or host name may change. Therefore, it's possible to have duplicate IP address or hostnames between records. At present, the combination of host name and IP address is considered to be a unique host.

The hostname is parsed from the run summary filename which is of the form ``<hostname>_<unix_timestamp>_run_summary.json``. The IP address and port number is parsed from the beam list attribute in the run summary (``beams.list``).

Columns containing host attributes are prefixed with the ``host.`` key.

--------------
Candidate data
--------------

Candidate data is extracted from the tab separated SPCCL file.

Columns containing generic (single or periodic pulse) parent candidate attributes are prefixed with the ``cand.`` key. Columns containing single pulse candidate attributes are prefixed with the ``sp_cand.`` key.
