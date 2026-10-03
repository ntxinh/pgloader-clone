"""
Migration orchestrator.

Phases (matching pgloader's pipeline):
 1. SCHEMA   – Create schemas, tables (without FKs & indexes)
 2. DATA     – COPY all tables in parallel (configurable workers)
 3. INDEXES  – Create non-PK indexes
 4. FKEYS    – Add foreign-key constraints
 5. RESET    – Reset sequences for identity columns
"""

from __future__ import annotations

import logging
import time
import threading
import queue
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional

import pymssql        # type: ignore
import psycopg        # type: ignore

from .introspect import introspect_table, list_tables, TableDef
from .ddl import (
    create_schema_ddl,
    create_table_ddl,
    create_fk_ddls,
    create_index_ddls,
)
from .copy_engine import copy_table

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass
class MssqlConfig:
    host: str
    port: int = 1433
    database: str = ""
    user: str = ""
    password: str = ""
    # TDS version; 7.4 works for Azure SQL
    tds_version: str = "7.4"
    # Azure SQL sometimes requires encryption
    encrypt: bool = True


@dataclass
class PgConfig:
    host: str
    port: int = 5432
    database: str = ""
    user: str = ""
    password: str = ""
    sslmode: str = "require"


@dataclass
class MigrationConfig:
    mssql: MssqlConfig
    pg: PgConfig
    # Which MSSQL schemas to migrate (None = all)
    include_schemas: Optional[list[str]] = None
    # Fully-qualified "schema.table" or just "table" names to skip
    exclude_tables: list[str] = field(default_factory=list)
    # Override pg schema name (None = keep source schema name)
    pg_schema_override: Optional[str] = None
    # Chunk size for COPY batches
    chunk_size: int = 50_000
    # Parallel copy workers
    workers: int = 4
    # Truncate target tables before COPY
    truncate: bool = False
    # Drop & recreate target tables
    drop_tables: bool = False


# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------

@dataclass
class TableResult:
    schema: str
    table: str
    rows: int = 0
    elapsed: float = 0.0
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.error is None


@dataclass
class MigrationResult:
    table_results: list[TableResult] = field(default_factory=list)
    schema_errors: list[str] = field(default_factory=list)
    total_elapsed: float = 0.0

    @property
    def rows_total(self) -> int:
        return sum(r.rows for r in self.table_results if r.ok)

    @property
    def failed_tables(self) -> list[TableResult]:
        return [r for r in self.table_results if not r.ok]


# ---------------------------------------------------------------------------
# Connection factories
# ---------------------------------------------------------------------------

def _mssql_connect(cfg: MssqlConfig):
    kwargs = dict(
        server=cfg.host,
        port=cfg.port,
        database=cfg.database,
        user=cfg.user,
        password=cfg.password,
        tds_version=cfg.tds_version,
        # pymssql uses 'login_timeout' and 'timeout' (seconds)
        login_timeout=30,
        timeout=0,          # no statement timeout
    )
    return pymssql.connect(**kwargs)


def _pg_connect(cfg: PgConfig):
    conn = psycopg.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.database,
        user=cfg.user,
        password=cfg.password,
        sslmode=cfg.sslmode,
        # Speed up bulk loads
        options="-c synchronous_commit=off",
    )
    conn.autocommit = False
    return conn


# ---------------------------------------------------------------------------
# Phase helpers
# ---------------------------------------------------------------------------

def _pg_schema_for(td: TableDef, override: Optional[str]) -> str:
    return override if override else td.schema


def _phase_schema(
    table_defs: list[TableDef],
    pg_cfg: PgConfig,
    cfg: MigrationConfig,
    errors: list[str],
) -> None:
    """Phase 1: Create schemas and tables (no FKs, no non-PK indexes)."""
    schemas_created: set[str] = set()

    with _pg_connect(pg_cfg) as conn:
        with conn.cursor() as cur:
            for td in table_defs:
                pg_schema = _pg_schema_for(td, cfg.pg_schema_override)

                if pg_schema not in schemas_created:
                    try:
                        cur.execute(create_schema_ddl(pg_schema))
                        schemas_created.add(pg_schema)
                        log.info("Schema '%s' ensured.", pg_schema)
                    except Exception as e:
                        msg = f"Error creating schema {pg_schema}: {e}"
                        log.error(msg)
                        errors.append(msg)
                        conn.rollback()
                        continue

                if cfg.drop_tables:
                    try:
                        cur.execute(
                            f'DROP TABLE IF EXISTS "{pg_schema}"."{td.name}" CASCADE'
                        )
                    except Exception:
                        conn.rollback()

                try:
                    ddl = create_table_ddl(td, pg_schema)
                    log.debug("DDL:\n%s", ddl)
                    cur.execute(ddl)
                    log.info("Table '%s'.'%s' created.", pg_schema, td.name)
                except Exception as e:
                    msg = f"Error creating table {pg_schema}.{td.name}: {e}"
                    log.error(msg)
                    errors.append(msg)
                    conn.rollback()

        conn.commit()


def _phase_indexes(
    table_defs: list[TableDef],
    pg_cfg: PgConfig,
    cfg: MigrationConfig,
    errors: list[str],
) -> None:
    """Phase 3: Non-PK indexes."""
    with _pg_connect(pg_cfg) as conn:
        with conn.cursor() as cur:
            for td in table_defs:
                pg_schema = _pg_schema_for(td, cfg.pg_schema_override)
                for stmt in create_index_ddls(td, pg_schema):
                    try:
                        cur.execute(stmt)
                    except Exception as e:
                        msg = f"Index error on {pg_schema}.{td.name}: {e}"
                        log.warning(msg)
                        errors.append(msg)
                        conn.rollback()
        conn.commit()


def _phase_fkeys(
    table_defs: list[TableDef],
    pg_cfg: PgConfig,
    cfg: MigrationConfig,
    errors: list[str],
) -> None:
    """Phase 4: Foreign keys."""
    with _pg_connect(pg_cfg) as conn:
        with conn.cursor() as cur:
            for td in table_defs:
                pg_schema = _pg_schema_for(td, cfg.pg_schema_override)
                for stmt in create_fk_ddls(td, pg_schema):
                    try:
                        cur.execute(stmt)
                    except Exception as e:
                        msg = f"FK error on {pg_schema}.{td.name}: {e}"
                        log.warning(msg)
                        errors.append(msg)
                        conn.rollback()
        conn.commit()


def _phase_reset_sequences(
    table_defs: list[TableDef],
    pg_cfg: PgConfig,
    cfg: MigrationConfig,
    errors: list[str],
) -> None:
    """Phase 5: Reset PostgreSQL sequences for identity columns."""
    with _pg_connect(pg_cfg) as conn:
        with conn.cursor() as cur:
            for td in table_defs:
                pg_schema = _pg_schema_for(td, cfg.pg_schema_override)
                for col in td.columns:
                    if col.is_identity:
                        try:
                            cur.execute(
                                f"""
                                SELECT setval(
                                    pg_get_serial_sequence(
                                        '"{pg_schema}"."{td.name}"',
                                        '{col.name}'
                                    ),
                                    COALESCE(
                                        (SELECT MAX("{col.name}")
                                         FROM "{pg_schema}"."{td.name}"),
                                        1
                                    )
                                )
                                """
                            )
                        except Exception as e:
                            msg = (f"Sequence reset error on "
                                   f"{pg_schema}.{td.name}.{col.name}: {e}")
                            log.warning(msg)
                            errors.append(msg)
                            conn.rollback()
        conn.commit()


# ---------------------------------------------------------------------------
# Data copy (parallel)
# ---------------------------------------------------------------------------

def _copy_one_table(
    td: TableDef,
    ms_cfg: MssqlConfig,
    pg_cfg: PgConfig,
    cfg: MigrationConfig,
) -> TableResult:
    """Copy a single table. Each worker owns its own connections."""
    result = TableResult(schema=td.schema, table=td.name)
    t0 = time.monotonic()
    ms_conn = None
    pg_conn = None
    try:
        ms_conn = _mssql_connect(ms_cfg)
        pg_conn = _pg_connect(pg_cfg)
        pg_schema = _pg_schema_for(td, cfg.pg_schema_override)

        rows = copy_table(
            ms_conn,
            pg_conn,
            td.schema,
            td.name,
            td,
            pg_schema,
            chunk_size=cfg.chunk_size,
            truncate=cfg.truncate,
        )
        pg_conn.commit()
        result.rows = rows
    except Exception as e:
        result.error = str(e)
        log.error("COPY failed for %s.%s: %s", td.schema, td.name, e, exc_info=True)
        if pg_conn:
            try:
                pg_conn.rollback()
            except Exception:
                pass
    finally:
        result.elapsed = time.monotonic() - t0
        for c in (ms_conn, pg_conn):
            if c:
                try:
                    c.close()
                except Exception:
                    pass
    return result


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_migration(
    cfg: MigrationConfig,
    progress_callback=None,
) -> MigrationResult:
    """
    Execute a full MSSQL → PostgreSQL migration.

    progress_callback(phase: str, current: int, total: int, message: str)
    is called at key points (optional).
    """
    result = MigrationResult()
    t_start = time.monotonic()

    def _progress(phase, current, total, message=""):
        if progress_callback:
            progress_callback(phase, current, total, message)

    # ---- Introspect source ---------------------------------------------------
    log.info("Connecting to MSSQL and introspecting schema…")
    ms_conn = _mssql_connect(cfg.mssql)
    try:
        pairs = list_tables(ms_conn, cfg.include_schemas, cfg.exclude_tables)
        log.info("Found %d tables to migrate.", len(pairs))

        table_defs: list[TableDef] = []
        for i, (schema, table) in enumerate(pairs):
            _progress("INTROSPECT", i + 1, len(pairs), f"{schema}.{table}")
            td = introspect_table(ms_conn, schema, table)
            table_defs.append(td)
    finally:
        ms_conn.close()

    if not table_defs:
        log.warning("No tables found – check --include-schema / --exclude-table.")
        result.total_elapsed = time.monotonic() - t_start
        return result

    # ---- Phase 1: Schema + tables -------------------------------------------
    log.info("Phase 1/5 – Creating schemas and tables…")
    _progress("SCHEMA", 0, len(table_defs), "")
    _phase_schema(table_defs, cfg.pg, cfg, result.schema_errors)
    _progress("SCHEMA", len(table_defs), len(table_defs), "done")

    # ---- Phase 2: COPY data (parallel) --------------------------------------
    log.info("Phase 2/5 – Copying data (%d workers)…", cfg.workers)
    completed = 0
    with ThreadPoolExecutor(max_workers=cfg.workers) as executor:
        futures = {
            executor.submit(_copy_one_table, td, cfg.mssql, cfg.pg, cfg): td
            for td in table_defs
        }
        for future in as_completed(futures):
            tr = future.result()
            result.table_results.append(tr)
            completed += 1
            status = f"OK {tr.rows:,} rows" if tr.ok else f"FAIL: {tr.error}"
            _progress("COPY", completed, len(table_defs),
                      f"{tr.schema}.{tr.table} – {status}")
            if tr.ok:
                log.info("✓ %s.%s – %d rows in %.1fs",
                         tr.schema, tr.table, tr.rows, tr.elapsed)
            else:
                log.error("✗ %s.%s – %s", tr.schema, tr.table, tr.error)

    # ---- Phase 3: Indexes ---------------------------------------------------
    log.info("Phase 3/5 – Creating indexes…")
    _progress("INDEX", 0, len(table_defs), "")
    _phase_indexes(table_defs, cfg.pg, cfg, result.schema_errors)
    _progress("INDEX", len(table_defs), len(table_defs), "done")

    # ---- Phase 4: Foreign keys ----------------------------------------------
    log.info("Phase 4/5 – Adding foreign keys…")
    _progress("FKEY", 0, len(table_defs), "")
    _phase_fkeys(table_defs, cfg.pg, cfg, result.schema_errors)
    _progress("FKEY", len(table_defs), len(table_defs), "done")

    # ---- Phase 5: Reset sequences -------------------------------------------
    log.info("Phase 5/5 – Resetting sequences…")
    _progress("SEQ", 0, len(table_defs), "")
    _phase_reset_sequences(table_defs, cfg.pg, cfg, result.schema_errors)
    _progress("SEQ", len(table_defs), len(table_defs), "done")

    result.total_elapsed = time.monotonic() - t_start
    return result
