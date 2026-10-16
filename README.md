# HubSpot dlt Ingestion Pipeline

This project extracts HubSpot CRM data with `dlt`, writes compressed Parquet to
MinIO, and loads the landed files into ClickHouse. Resources are fetched in
parallel, and records with update timestamps use dlt incremental cursors and
append-only Parquet files. dlt's local pipeline state is the recovery checkpoint; the SQLite
control database is for pause/resume and visible page progress.

## Setup

Use Python 3.10 or newer, install `requirements.txt`, and provide:

```sh
export HUBSPOT_ACCESS_TOKEN='...'
export MINIO_ENDPOINT='http://localhost:9000'
export MINIO_BUCKET='hubspot'
export MINIO_PREFIX='hubspot'
export MINIO_ACCESS_KEY='minioadmin'
export MINIO_SECRET_KEY='minioadmin'
export CLICKHOUSE_HOST='localhost'
```

The default state locations are `.dlt/` for dlt's durable pipeline state and
`state/control.sqlite` for operator controls. Keep both on persistent storage
when running in a container. The Parquet layout is
`hubspot/{resource}/year=YYYY/month=MM/` within the configured bucket.
Extraction defaults to eight worker threads; set `HUBSPOT_WORKERS` to tune it.

## Run and Operate

```sh
python -m hubspot_dlt.cli run
python -m hubspot_dlt.cli pause
python -m hubspot_dlt.cli resume
python -m hubspot_dlt.cli status
python -m hubspot_dlt.cli load-clickhouse
```

Pause takes effect between API pages. Run `pause`, `resume`, and `status` from
another process while ingestion is active; they share the SQLite database. Page
offsets and observed update timestamps are reported for operations. They are
not used as the recovery cursor: dlt commits incremental state with a successful
load package. A forced process termination therefore replays any uncommitted
work from dlt's last committed state. ClickHouse uses `ReplacingMergeTree` with
the HubSpot update timestamp as its version and object ID as its key, so replayed
Parquet rows resolve to one latest logical row (`FINAL`). Do not delete `.dlt/`
between runs.

The source covers contacts, companies, deals (including company associations),
tickets, line items, engagements, deal pipelines, and owners. HubSpot API access
must include read scopes for these CRM object types. HTTP 429 and transient
server errors are retried.

## ClickHouse

The loader reads the Parquet glob directly from MinIO using ClickHouse's `s3`
table function. It creates `hubspot_{resource}` tables using
`ReplacingMergeTree(_version)`, then creates `v_hubspot_deal_pipeline`. The view joins
deals to their associated companies and deal pipelines and uses `FINAL` reads
to expose the latest row per object ID after replayed loads.

```sql
SELECT * FROM default.v_hubspot_deal_pipeline LIMIT 20;
```

The local test suite includes a synthetic extraction that is force-killed with
`SIGKILL`, restarted from the same dlt state directory, and checked for exactly
one landed row per ID. It also synchronizes all eight resource iterators at a
thread barrier to verify that independent resources execute concurrently. Run
both checks with `python -m unittest discover -s tests`.

## Local Checks

```sh
python -m unittest discover -s tests
python -m compileall -q hubspot_dlt tests
```
