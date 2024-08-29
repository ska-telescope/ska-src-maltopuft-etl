# ska-src-maltopuft-etl

The MAchine Learning TOolkit for PUlsars and Fast Transients (MALTOPUFT) facilitates the enhancement of real-time single pulse and periodicity searches at the Square Kilometer Array (SKA).

MALTOPUFTDB is the PostgreSQL instance used by several toolkit components. This repository constains source code for all MALTOPUFT ETL pipelines, which are responsible for extracting data archives from several sources and loading standardised data into MALTOPUFTDB.

Please refer to the [project documentation](https://developer.skao.int/projects/ska-src-maltopuft-etl/en/latest/index.html) for more information.

## Quick start

To get started with loading MeerTRAP archive and ATNF catalogue data into a MALTOPUFTDB instance, you can clone this repository with:

```bash
git clone git@gitlab.com:ska-telescope/src/maltopuft/ska-src-maltopuft-etl.git
```

The package can be installed in a virtual environment with:

```bash
cd ska-src-maltopuft-etl
python3 -m venv .venv
source .venv/bin/activate
poetry install
```

A copy of the example environment variables can be used during development. Ensure that the `MALTOPUFT_POSTGRES_*` variables point to your MALTOPUFT DB instance:

```bash
cp .env.example .env
```

Create a `./data` directory at the root level of the project and fill it with MeerTRAP archival data:

```bash
mkdir data

# ... Add the MeerTRAP data archive
```

MeerTRAP and ATNF data can be loaded into a MALTOPUFTDB instance with:

```bash
# Load MeerTRAP data
python src/main.py

# Load ATNF data
python src/atnf/atnf.py
```

Run a simple query to ensure that results are returned:

```bash
psql -U postgres -d maltopuftdb -c 'SELECT * FROM sp_candidate LIMIT 1;'
```
