"""
MSSQL → PostgreSQL type mapping.

Rules follow pgloader's mapping table as closely as possible.
All MSSQL type names come back lower-cased from INFORMATION_SCHEMA.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class PgType:
    """Resolved PostgreSQL column type plus any COPY-time caveats."""
    ddl: str                            # e.g. "numeric(18,4)"
    needs_text_cast: bool = False       # True for types that must be sent as text in COPY CSV


# ---------------------------------------------------------------------------
# Static mapping: mssql_type → pg_ddl (before applying precision/scale)
# ---------------------------------------------------------------------------
_BASE: dict[str, str] = {
    # Exact numerics
    "bit":              "boolean",
    "tinyint":          "smallint",
    "smallint":         "smallint",
    "int":              "integer",
    "bigint":           "bigint",
    "decimal":          "numeric",
    "numeric":          "numeric",
    "money":            "numeric(19,4)",
    "smallmoney":       "numeric(10,4)",

    # Approximate numerics
    "float":            "double precision",
    "real":             "real",

    # Date / time
    "date":             "date",
    "time":             "time",
    "datetime":         "timestamp",
    "datetime2":        "timestamp",
    "smalldatetime":    "timestamp",
    "datetimeoffset":   "timestamptz",

    # Character strings
    "char":             "char",
    "varchar":          "varchar",
    "text":             "text",
    "nchar":            "char",
    "nvarchar":         "varchar",
    "ntext":            "text",

    # Binary
    "binary":           "bytea",
    "varbinary":        "bytea",
    "image":            "bytea",

    # Unicode / misc
    "uniqueidentifier": "uuid",
    "xml":              "xml",
    "sql_variant":      "text",         # best-effort
    "hierarchyid":      "text",         # best-effort
    "geography":        "text",         # best-effort (use PostGIS in a second pass)
    "geometry":         "text",         # best-effort
    "rowversion":       "bytea",
    "timestamp":        "bytea",        # MSSQL 'timestamp' is rowversion, NOT a datetime
}

# Types that must be emitted as text in COPY (psycopg handles the rest)
_TEXT_IN_COPY: set[str] = {"xml", "sql_variant", "hierarchyid", "geography", "geometry"}


def resolve(
    mssql_type: str,
    char_max_length: Optional[int],
    numeric_precision: Optional[int],
    numeric_scale: Optional[int],
) -> PgType:
    """
    Return the PostgreSQL DDL string for an MSSQL column.

    Parameters
    ----------
    mssql_type:
        Lower-cased MSSQL data type name from INFORMATION_SCHEMA.
    char_max_length:
        CHARACTER_MAXIMUM_LENGTH from INFORMATION_SCHEMA (may be -1 for MAX).
    numeric_precision / numeric_scale:
        NUMERIC_PRECISION / NUMERIC_SCALE (may be None).
    """
    base = _BASE.get(mssql_type, "text")
    needs_text_cast = mssql_type in _TEXT_IN_COPY

    # --- character types ---
    if mssql_type in ("char", "nchar", "varchar", "nvarchar"):
        if char_max_length is None or char_max_length == -1:
            # MAX → text (PostgreSQL has no varchar(MAX))
            base = "text"
        elif mssql_type in ("char", "nchar"):
            base = f"char({char_max_length})"
        else:
            base = f"varchar({char_max_length})"

    # --- binary types ---
    elif mssql_type in ("binary", "varbinary"):
        # keep bytea regardless of length
        base = "bytea"

    # --- exact numerics with precision/scale ---
    elif mssql_type in ("decimal", "numeric"):
        if numeric_precision is not None and numeric_scale is not None:
            base = f"numeric({numeric_precision},{numeric_scale})"
        elif numeric_precision is not None:
            base = f"numeric({numeric_precision})"
        else:
            base = "numeric"

    # --- float ---
    elif mssql_type == "float":
        # MSSQL float(n): n<=24 → real, n>24 → double precision
        if numeric_precision is not None and numeric_precision <= 24:
            base = "real"
        else:
            base = "double precision"

    # --- time with precision ---
    elif mssql_type == "time":
        if numeric_scale is not None:
            base = f"time({numeric_scale})"

    elif mssql_type == "datetime2":
        if numeric_scale is not None:
            base = f"timestamp({numeric_scale})"

    elif mssql_type == "datetimeoffset":
        if numeric_scale is not None:
            base = f"timestamptz({numeric_scale})"

    return PgType(ddl=base, needs_text_cast=needs_text_cast)
