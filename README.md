# pgloader-py

A high-performance **MSSQL → PostgreSQL** migration tool written in Python,
modelled on [pgloader](https://pgloader.io/).

---

## Why not just use pgloader?

* pgloader's Docker image doesn't include Azure's root CA by default, causing
  SSL errors (`X509_V_ERR_SELF_SIGNED_CERT_IN_CHAIN`).
* This tool uses **pure Python** — easy to customise, containerise, or embed in
  a pipeline.

## Core design

| Concern | Approach |
|---|---|
| **No INSERT** | All data transfer uses PostgreSQL `COPY … FROM STDIN (FORMAT CSV)` — the fastest bulk-load path |
| **Streaming** | MSSQL is read with a server-side cursor in configurable chunks (default 50 000 rows) |
| **Parallelism** | Tables are copied in parallel using a thread pool (default 4 workers) |
| **Type safety** | Full MSSQL → PostgreSQL type mapping with precision/scale/length |
| **Schema fidelity** | PK, UNIQUE, FK, non-clustered indexes, CHECK constraints, DEFAULT expressions |

## Installation

Requires **Python ≥ 3.11** and [uv](https://github.com/astral-sh/uv).

```bash
# Clone / unzip the project
cd pgloader-py

# Install dependencies and the CLI
uv sync
```

The CLI is then available as:

```bash
uv run pgloader-py --help
```

Or install globally:

```bash
uv tool install .
pgloader-py --help
```

## Quick start

```bash
# 1. Copy the example env file and fill in your credentials
cp .env.example .env
$EDITOR .env

# 2. Test connectivity first
uv run pgloader-py test-connection

# 3. Run the migration
uv run pgloader-py migrate
```

All `--option` flags can also be provided via environment variables or the
`.env` file (see `.env.example`).

## Full CLI reference

### `pgloader-py migrate`

```
Options:
  --ms-host TEXT        MSSQL server host            [env: MS_HOST]
  --ms-port INTEGER     default: 1433                [env: MS_PORT]
  --ms-db TEXT          MSSQL database name          [env: MS_DATABASE]
  --ms-user TEXT                                     [env: MS_USER]
  --ms-password TEXT                                 [env: MS_PASSWORD]
  --ms-tds TEXT         TDS version, default 7.4     [env: MS_TDS]

  --pg-host TEXT        PostgreSQL host              [env: PG_HOST]
  --pg-port INTEGER     default: 5432                [env: PG_PORT]
  --pg-db TEXT          PostgreSQL database name     [env: PG_DATABASE]
  --pg-user TEXT                                     [env: PG_USER]
  --pg-password TEXT                                 [env: PG_PASSWORD]
  --pg-sslmode TEXT     default: require             [env: PG_SSLMODE]

  --include-schema TEXT Only migrate these schemas (repeatable)
  --exclude-table TEXT  Skip these tables (repeatable, e.g. dbo.Staging)
  --pg-schema TEXT      Map all schemas → single PG schema
  --workers INTEGER     Parallel workers, default 4
  --chunk-size INTEGER  Rows per COPY batch, default 50000
  --truncate            TRUNCATE target tables before COPY
  --drop-tables         DROP + re-create target tables
  -v, --verbose         Debug logging
```

### `pgloader-py test-connection`

Verifies connectivity to both databases and prints server version strings.

## Migration phases

```
1. INTROSPECT  Read table/column/constraint metadata from MSSQL
2. SCHEMA      CREATE SCHEMA + CREATE TABLE (PK + UNIQUE inline)
3. DATA        COPY all tables in parallel
4. INDEX       CREATE INDEX (non-PK)
5. FKEY        ALTER TABLE … ADD CONSTRAINT … FOREIGN KEY
6. SEQ         Reset PostgreSQL identity sequences
```

Foreign keys are added **after** data load so they don't slow down COPY.

## Type mapping cheatsheet

| MSSQL | PostgreSQL |
|---|---|
| `bit` | `boolean` |
| `tinyint` | `smallint` |
| `int` / `bigint` | `integer` / `bigint` |
| `decimal(p,s)` / `numeric(p,s)` | `numeric(p,s)` |
| `money` | `numeric(19,4)` |
| `float` | `double precision` |
| `varchar(n)` / `nvarchar(n)` | `varchar(n)` |
| `varchar(max)` / `nvarchar(max)` | `text` |
| `uniqueidentifier` | `uuid` |
| `datetime` / `datetime2` | `timestamp` |
| `datetimeoffset` | `timestamptz` |
| `varbinary` / `image` | `bytea` |
| `xml` | `xml` |
| `geography` / `geometry` | `text` (best-effort) |

## Azure SQL SSL notes

Azure SQL requires TLS. Set `--pg-sslmode require` (default) for Azure
PostgreSQL Flexible Server.

If you hit `SSL SYSCALL error: EOF detected`, add:

```
PG_SSLMODE=require
```

For strict certificate verification (recommended in production):

```
PG_SSLMODE=verify-full
```

## Performance tuning

| Knob | Recommendation |
|---|---|
| `--workers` | Match to number of vCores on your migration host (4–16) |
| `--chunk-size` | Larger = fewer round-trips; smaller = lower memory. 50 000 is a good default. |
| PostgreSQL `synchronous_commit` | Already set to `off` per-session for speed |
| Azure SQL firewall | Ensure your migration host IP is whitelisted |
| Same Azure region | Run the migration tool in the same region as both databases |

## Running in Docker

```dockerfile
FROM python:3.12-slim
RUN pip install uv
WORKDIR /app
COPY . .
RUN uv sync --frozen
ENTRYPOINT ["uv", "run", "pgloader-py"]
```

```bash
docker build -t pgloader-py .
docker run --env-file .env pgloader-py migrate
```

---

```sh
# 1. Install
uv sync

# 2. Test connections
cp .env.example .env && $EDITOR .env
uv run pgloader-py test-connection

# 3. Migrate
uv run pgloader-py migrate --workers 8 --truncate
```