"""
DDL generation: MSSQL TableDef → PostgreSQL CREATE TABLE statements.
"""

from __future__ import annotations

import re
from typing import Optional

from .introspect import TableDef, ColumnDef
from .type_mapping import resolve


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _quote(name: str) -> str:
    """Double-quote an identifier."""
    return f'"{name}"'


def _clean_default(mssql_default: Optional[str]) -> Optional[str]:
    """
    Convert MSSQL default expression to something PostgreSQL understands.
    Returns None to suppress the DEFAULT clause entirely when we can't translate.
    """
    if mssql_default is None:
        return None

    expr = mssql_default.strip()

    # Strip outer parens that MSSQL loves: ((0)) → 0,  ('hello') → 'hello'
    while expr.startswith("(") and expr.endswith(")"):
        inner = expr[1:-1].strip()
        # Make sure we're not stripping a function call like (getdate())
        if inner.count("(") == inner.count(")"):
            expr = inner
        else:
            break

    lower = expr.lower()

    # MSSQL functions → PostgreSQL equivalents
    replacements = {
        "getdate()":            "CURRENT_TIMESTAMP",
        "getutcdate()":         "(NOW() AT TIME ZONE 'UTC')",
        "sysdatetime()":        "CURRENT_TIMESTAMP",
        "sysutcdatetime()":     "(NOW() AT TIME ZONE 'UTC')",
        "sysdatetimeoffset()":  "CURRENT_TIMESTAMP",
        "newid()":              "gen_random_uuid()",
        "newsequentialid()":    "gen_random_uuid()",
    }
    if lower in replacements:
        return replacements[lower]

    # Numeric literals  →  keep as-is
    if re.fullmatch(r"-?\d+(\.\d+)?", expr):
        return expr

    # String literals  →  keep as-is ('foo')
    if expr.startswith("'") and expr.endswith("'"):
        return expr

    # Boolean-ish: 0/1 → false/true for BIT columns handled at column level
    # For other columns we just pass through
    return expr


def _column_ddl(col: ColumnDef) -> str:
    """Return the full column DDL line (without trailing comma)."""
    # Skip computed columns — PostgreSQL doesn't support stored computed
    # columns the same way; emit a plain nullable column as placeholder.
    if col.computed_def is not None:
        # Best we can do without analysing the expression:
        return f"    {_quote(col.name)} text  -- COMPUTED: {col.computed_def}"

    pg = resolve(col.mssql_type, col.char_max_length,
                 col.numeric_precision, col.numeric_scale)

    parts = [f"    {_quote(col.name)}", pg.ddl]

    # Identity → GENERATED ALWAYS AS IDENTITY
    if col.is_identity:
        parts.append("GENERATED ALWAYS AS IDENTITY")

    # Nullability
    if not col.is_nullable and not col.is_identity:
        parts.append("NOT NULL")

    # Default
    if col.default_expr and not col.is_identity:
        pg_default = _clean_default(col.default_expr)
        if pg_default is not None:
            # BIT default 0/1 → false/true
            if col.mssql_type == "bit":
                pg_default = "true" if pg_default.strip() in ("1", "true") else "false"
            parts.append(f"DEFAULT {pg_default}")

    return " ".join(parts)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def create_schema_ddl(pg_schema: str) -> str:
    return f'CREATE SCHEMA IF NOT EXISTS "{pg_schema}";\n'


def create_table_ddl(td: TableDef, pg_schema: Optional[str] = None) -> str:
    """Return CREATE TABLE DDL for the given TableDef."""
    schema = pg_schema or td.schema
    lines = []

    col_lines = [_column_ddl(c) for c in td.columns]

    # Primary key
    if td.primary_keys:
        pk_cols = ", ".join(_quote(c) for c in td.primary_keys)
        col_lines.append(f"    CONSTRAINT {_quote('pk_' + td.name)} PRIMARY KEY ({pk_cols})")

    # Unique constraints
    for i, uc in enumerate(td.unique_constraints):
        uc_cols = ", ".join(_quote(c) for c in uc)
        col_lines.append(
            f"    CONSTRAINT {_quote('uq_' + td.name + '_' + str(i))} UNIQUE ({uc_cols})"
        )

    body = ",\n".join(col_lines)
    lines.append(
        f'CREATE TABLE IF NOT EXISTS "{schema}"."{td.name}" (\n{body}\n);'
    )
    return "\n".join(lines) + "\n"


def create_fk_ddls(td: TableDef, pg_schema: Optional[str] = None) -> list[str]:
    """Return ALTER TABLE … ADD CONSTRAINT … FOREIGN KEY statements."""
    schema = pg_schema or td.schema
    stmts = []
    for fk in td.foreign_keys:
        src_cols = ", ".join(_quote(c) for c in fk["src_cols"])
        ref_cols = ", ".join(_quote(c) for c in fk["ref_cols"])
        ref_schema = pg_schema or fk["ref_schema"]
        stmt = (
            f'ALTER TABLE "{schema}"."{td.name}"\n'
            f'  ADD CONSTRAINT "{fk["name"]}"\n'
            f'  FOREIGN KEY ({src_cols})\n'
            f'  REFERENCES "{ref_schema}"."{fk["ref_table"]}" ({ref_cols})\n'
            f'  ON DELETE {fk["on_delete"]} ON UPDATE {fk["on_update"]};\n'
        )
        stmts.append(stmt)
    return stmts


def create_index_ddls(td: TableDef, pg_schema: Optional[str] = None) -> list[str]:
    """Return CREATE INDEX statements (non-PK, non-UK)."""
    schema = pg_schema or td.schema
    stmts = []
    for idx in td.indexes:
        unique = "UNIQUE " if idx["is_unique"] else ""
        cols = ", ".join(_quote(c) for c in idx["columns"])
        stmt = (
            f'CREATE {unique}INDEX IF NOT EXISTS "{idx["name"]}"\n'
            f'  ON "{schema}"."{td.name}" ({cols});\n'
        )
        stmts.append(stmt)
    return stmts
