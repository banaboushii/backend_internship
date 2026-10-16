# HubSpot dlt Ingestion Pipeline

This project extracts HubSpot CRM data with `dlt`, writes compressed Parquet to
MinIO, and loads the landed files into ClickHouse. Resources are fetched in
parallel, and records with update timestamps use dlt incremental cursors and
append-only Parquet files. dlt's local pipeline state is the recovery checkpoint; the SQLite
control database is for pause/resume and visible page progress.

## Setup

Use Python 3.10 or newer, install Docker Compose and Python requirements. Start
the local MinIO and ClickHouse services:

```sh
cp .env.example .env
```

Set `HUBSPOT_ACCESS_TOKEN` in `.env` to a HubSpot private app token, then run:

```sh
docker compose up -d --wait
python -m pip install -r requirements.txt
```

The Compose stack creates the MinIO bucket automatically. MinIO's API and
console are available at `http://localhost:9000` and `http://localhost:9001`;
ClickHouse HTTP is at `http://localhost:8123`. The default MinIO credentials
are for local development only; change them before exposing these services.
Do not commit your `.env` file.

Run the Python pipeline from the host as shown below. It connects to MinIO at
`localhost:9000` and ClickHouse at `localhost:8123`. The loader uses
`CLICKHOUSE_S3_ENDPOINT=http://minio:9000` so the ClickHouse container can reach
MinIO over the Compose network.

### Run without HubSpot credentials

The repository includes a paginated local HubSpot API simulator:

```sh
# In .env, use these values for a credential-free demo:
# HUBSPOT_ACCESS_TOKEN=mock-token
# HUBSPOT_API_BASE=http://127.0.0.1:8000
python -m hubspot_dlt.mock_api
```

In a second terminal, start the pipeline and then load the resulting Parquet
files into ClickHouse:

```sh
python -m hubspot_dlt.cli run
python -m hubspot_dlt.cli load-clickhouse
```

The mock API returns paginated synthetic records for all eight resources.
`HUBSPOT_MOCK_RECORDS`, `HUBSPOT_MOCK_PAGE_SIZE`, and
`HUBSPOT_MOCK_DELAY_SECONDS` control its volume and pace. To use real HubSpot
data, restore `HUBSPOT_API_BASE=https://api.hubapi.com` and set a valid private
app token.

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

Contacts, companies, deals, tickets, and line items use the HubSpot Search API
with an `hs_lastmodifieddate > cursor` filter after their first full sync.
Engagements use their API's `since` parameter. HubSpot's owners and pipelines
endpoints do not expose a compatible modified-since filter here, so those two
resources are refreshed as snapshots.

## ClickHouse

The loader reads Parquet files from MinIO using ClickHouse's `s3` table
function. It lists files in MinIO and imports them one at a time. Each row
stores its source object key, and the insert excludes keys already present in
the table, so rerunning the loader does not append the same file again. New
Parquet fields are added as nullable string columns before that file is loaded.
Empty resources still get a typed table; no `s3()` read is attempted when there
are no files. The loader creates `hubspot_{resource}` tables using
`ReplacingMergeTree(_version)`, then creates `v_hubspot_deal_pipeline`. The view joins
deals to their associated companies and deal pipelines and uses `FINAL` reads
to expose the latest row per object ID after replayed loads.

```sql
SELECT * FROM default.v_hubspot_deal_pipeline LIMIT 20;
```

Stop the services with `docker compose down`. Persistent MinIO and ClickHouse
data remain in named Docker volumes; `docker compose down -v` also removes that
data.

### Recording the required demo

The video must be recorded by a project owner; it cannot be generated from this
workspace. With the mock API and Compose stack running, record these steps:

1. Run `python -m hubspot_dlt.cli run` and show successful completion.
2. Open the MinIO console at `http://localhost:9001`, sign in using the MinIO
   credentials in `.env`, and show Parquet files in the `hubspot` bucket.
3. Start another run with a larger `HUBSPOT_MOCK_RECORDS` value and a delay,
   then run `python -m hubspot_dlt.cli pause` in another terminal. Show
   `python -m hubspot_dlt.cli status`, run `resume`, and show the status again.
4. Start a run, find its PID with `ps`, terminate it with `kill -9 <PID>`, then
   restart the pipeline and show that dlt resumes from its persisted state.
5. Run `python -m hubspot_dlt.cli load-clickhouse`, then query
   `SELECT * FROM default.v_hubspot_deal_pipeline LIMIT 20` using
   `clickhouse-client` or the ClickHouse HTTP interface.

The local test suite checks mock API pagination and incremental search,
ClickHouse file-import retry behavior, schema evolution, and dlt recovery after
a forced kill while the destination is uploading a Parquet file. It also
synchronizes all eight resource iterators at a thread barrier to verify parallel
extraction. Run the checks with `python -m unittest discover -s tests`.

## Local Checks

```sh
python -m unittest discover -s tests
python -m compileall -q hubspot_dlt tests
```
