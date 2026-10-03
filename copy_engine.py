"""
High-performance data copy engine.

Strategy
--------
1. Open a server-side cursor on MSSQL and stream rows in chunks.
2. For each chunk, serialize rows to CSV in memory (io.StringIO or io.BytesIO).
3. Feed the buffer directly into PostgreSQL via COPY … FROM STDIN (CSV).

This avoids any INSERT statement and matches pgloader's core strategy.

Type coercions handled here
---------------------------
* BIT (MSSQL) → boolean ('t'/'f' in CSV)
* VARBINARY / IMAGE / BINARY → hex-escaped '\\x…' in text, psycopg reads as bytea
* UNIQUEIDENTIFIER → lowercase UUID string (PostgreSQL is case-insensitive but
  canonical form is lower)
* datetime / datetime2 / datetimeoffset → ISO-8601 string
* None → empty field (PostgreSQL NULL in CSV)
* Computed columns → excluded from SELECT
"""

from __future__ import annotations

import io
import csv
import logging
import datetime
from decimal import Decimal
from typing import Any, Iterator

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Value serialisation helpers
# ---------------------------------------------------------------------------

_CSV_NULL = ""          # empty field = NULL in COPY CSV with default NULL ''


def _bytes_to_pg(b: bytes) -> str:
    """Convert Python bytes to PostgreSQL hex literal: \\xDEADBEEF"""
    return "\\x" + b.hex()


def _coerce(value: Any, mssql_type: str) -> str:
    """
    Convert a single Python value (as returned by pymssql) to a CSV string
    token suitable for PostgreSQL COPY CSV.
    Returns _CSV_NULL for SQL NULL.
    """
    if value is None:
        return _CSV_NULL

    t = mssql_type

    # --- boolean ---
    if t == "bit":
        return "t" if value else "f"

    # --- binary ---
    if t in ("binary", "varbinary", "image", "rowversion", "timestamp"):
        if isinstance(value, (bytes, bytearray, memoryview)):
            return _bytes_to_pg(bytes(value))
        return _CSV_NULL

    # --- UUID ---
    if t == "uniqueidentifier":
        return str(value).lower()

    # --- datetime types ---
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()

    # --- datetimeoffset comes as string from pymssql with tz info ---
    if t == "datetimeoffset" and isinstance(value, str):
        return value

    # --- Decimal → string without scientific notation ---
    if isinstance(value, Decimal):
        return str(value)

    # --- everything else: let Python str() handle it ---
    return str(value)


# ---------------------------------------------------------------------------
# Per-table copy worker
# ---------------------------------------------------------------------------

def _build_select(schema: str, table: str, columns) -> str:
    """
    Build a SELECT that:
    * Excludes computed columns (they have no data source).
    * Casts MSSQL-specific types that pymssql can't deserialise natively.
    """
    select_parts = []
    for col in columns:
        if col.computed_def is not None:
            # Don't select computed columns – excluded from COPY target too
            continue
        quoted = f"[{col.name}]"
        t = col.mssql_type
        if t == "datetimeoffset":
            # Force conversion to string with tz; pymssql returns offset-aware string
            select_parts.append(f"CONVERT(varchar(50), {quoted}, 127) AS [{col.name}]")
        elif t in ("xml", "sql_variant", "hierarchyid", "geography", "geometry"):
            select_parts.append(f"CAST({quoted} AS nvarchar(max)) AS [{col.name}]")
        else:
            select_parts.append(quoted)

    cols_sql = ", ".join(select_parts)
    return f"SELECT {cols_sql} FROM [{schema}].[{table}] WITH (NOLOCK)"


def _rows_to_csv_buffer(rows, columns_meta: list[tuple[str, str]]) -> io.BytesIO:
    """
    Serialise a list of rows to a UTF-8 CSV buffer suitable for COPY.
    columns_meta: list of (col_name, mssql_type) for non-computed columns.
    """
    buf = io.StringIO()
    writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
    for row in rows:
        writer.writerow(
            _coerce(val, mtype) for val, (_, mtype) in zip(row, columns_meta)
        )
    return io.BytesIO(buf.getvalue().encode("utf-8"))


def copy_table(
    mssql_conn,
    pg_conn,
    schema: str,
    table: str,
    table_def,
    pg_schema: str,
    chunk_size: int = 50_000,
    *,
    truncate: bool = False,
) -> int:
    """
    Stream all rows from MSSQL [schema].[table] → PostgreSQL "pg_schema"."table"
    using COPY … FROM STDIN (CSV).

    Returns total rows copied.
    """
    # Identify copyable columns (exclude computed)
    copyable = [c for c in table_def.columns if c.computed_def is None]
    if not copyable:
        log.warning("Table %s.%s has no copyable columns – skipped.", schema, table)
        return 0

    columns_meta = [(c.name, c.mssql_type) for c in copyable]
    pg_col_list = ", ".join(f'"{c.name}"' for c in copyable)
    copy_sql = (
        f'COPY "{pg_schema}"."{table}" ({pg_col_list}) '
        f"FROM STDIN (FORMAT CSV, NULL '')"
    )
    select_sql = _build_select(schema, table, table_def.columns)

    total = 0

    with pg_conn.cursor() as pg_cur:
        # Optionally truncate target first (keeps identity sequences)
        if truncate:
            pg_cur.execute(f'TRUNCATE TABLE "{pg_schema}"."{table}" CASCADE')

        # Open server-side cursor on MSSQL for memory-efficient streaming
        ms_cur = mssql_conn.cursor()
        ms_cur.execute(select_sql)

        try:
            while True:
                rows = ms_cur.fetchmany(chunk_size)
                if not rows:
                    break

                buf = _rows_to_csv_buffer(rows, columns_meta)
                buf.seek(0)

                with pg_cur.copy(copy_sql) as copy:
                    while True:
                        data = buf.read(65536)
                        if not data:
                            break
                        copy.write(data)

                total += len(rows)
                log.debug("  … %d rows copied so far", total)
        finally:
            ms_cur.close()

    return total
