"""
MSSQL schema introspection.

Reads tables, columns, primary keys, unique constraints, foreign keys,
indexes, check constraints, and default constraints from the source DB.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger(__name__)


@dataclass
class ColumnDef:
    name: str
    ordinal: int
    mssql_type: str
    char_max_length: Optional[int]
    numeric_precision: Optional[int]
    numeric_scale: Optional[int]
    is_nullable: bool
    is_identity: bool
    default_expr: Optional[str]     # raw MSSQL default, e.g. "((0))" or "('hello')"
    computed_def: Optional[str]     # for computed columns (skipped in data copy)


@dataclass
class TableDef:
    schema: str
    name: str
    columns: list[ColumnDef] = field(default_factory=list)
    primary_keys: list[str] = field(default_factory=list)      # ordered column names
    unique_constraints: list[list[str]] = field(default_factory=list)
    foreign_keys: list[dict] = field(default_factory=list)
    indexes: list[dict] = field(default_factory=list)
    check_constraints: list[dict] = field(default_factory=list)

    @property
    def qualified(self) -> str:
        return f"{self.schema}.{self.name}"


# ---------------------------------------------------------------------------
# Introspection queries
# ---------------------------------------------------------------------------

_COLUMNS_SQL = """
SELECT
    c.COLUMN_NAME,
    c.ORDINAL_POSITION,
    c.DATA_TYPE,
    c.CHARACTER_MAXIMUM_LENGTH,
    c.NUMERIC_PRECISION,
    c.NUMERIC_SCALE,
    c.IS_NULLABLE,
    COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') AS is_identity,
    c.COLUMN_DEFAULT,
    cc.definition AS computed_definition
FROM INFORMATION_SCHEMA.COLUMNS c
LEFT JOIN sys.computed_columns cc
    ON cc.object_id = OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME)
    AND cc.name = c.COLUMN_NAME
WHERE c.TABLE_SCHEMA = %s
  AND c.TABLE_NAME   = %s
ORDER BY c.ORDINAL_POSITION
"""

_PK_SQL = """
SELECT kcu.COLUMN_NAME
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
    AND tc.TABLE_SCHEMA   = kcu.TABLE_SCHEMA
WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
  AND tc.TABLE_SCHEMA    = %s
  AND tc.TABLE_NAME      = %s
ORDER BY kcu.ORDINAL_POSITION
"""

_UNIQUE_SQL = """
SELECT tc.CONSTRAINT_NAME, kcu.COLUMN_NAME
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
    AND tc.TABLE_SCHEMA   = kcu.TABLE_SCHEMA
WHERE tc.CONSTRAINT_TYPE = 'UNIQUE'
  AND tc.TABLE_SCHEMA    = %s
  AND tc.TABLE_NAME      = %s
ORDER BY tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
"""

_FK_SQL = """
SELECT
    fk.name  AS fk_name,
    SCHEMA_NAME(fk.schema_id)                AS src_schema,
    OBJECT_NAME(fk.parent_object_id)         AS src_table,
    COL_NAME(fkc.parent_object_id,  fkc.parent_column_id)     AS src_col,
    SCHEMA_NAME(pk_t.schema_id)              AS ref_schema,
    OBJECT_NAME(fk.referenced_object_id)     AS ref_table,
    COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS ref_col,
    fkc.constraint_column_id AS ordinal,
    CASE fk.delete_referential_action WHEN 1 THEN 'CASCADE'
         WHEN 2 THEN 'SET NULL' WHEN 3 THEN 'SET DEFAULT'
         ELSE 'NO ACTION' END AS on_delete,
    CASE fk.update_referential_action WHEN 1 THEN 'CASCADE'
         WHEN 2 THEN 'SET NULL' WHEN 3 THEN 'SET DEFAULT'
         ELSE 'NO ACTION' END AS on_update
FROM sys.foreign_keys fk
JOIN sys.foreign_key_columns fkc
    ON fk.object_id = fkc.constraint_object_id
JOIN sys.tables pk_t
    ON pk_t.object_id = fk.referenced_object_id
WHERE SCHEMA_NAME(fk.schema_id) = %s
  AND OBJECT_NAME(fk.parent_object_id) = %s
ORDER BY fk.name, fkc.constraint_column_id
"""

_INDEX_SQL = """
SELECT
    i.name AS index_name,
    i.is_unique,
    i.type_desc,
    STRING_AGG(c.name, ',' ) WITHIN GROUP (ORDER BY ic.key_ordinal) AS columns
FROM sys.indexes i
JOIN sys.index_columns ic
    ON ic.object_id = i.object_id AND ic.index_id = i.index_id
JOIN sys.columns c
    ON c.object_id = i.object_id AND c.column_id = ic.column_id
WHERE i.object_id = OBJECT_ID(%s)
  AND i.is_primary_key = 0
  AND i.type > 0
  AND ic.is_included_column = 0
GROUP BY i.name, i.is_unique, i.type_desc
"""

_CHECK_SQL = """
SELECT cc.name, cc.definition
FROM sys.check_constraints cc
WHERE cc.parent_object_id = OBJECT_ID(%s)
"""

_TABLES_SQL = """
SELECT TABLE_SCHEMA, TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE'
  AND TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA')
ORDER BY TABLE_SCHEMA, TABLE_NAME
"""


def list_tables(conn, include_schemas: list[str] | None, exclude_tables: list[str]) -> list[tuple[str, str]]:
    """Return [(schema, table), …] from the source MSSQL database."""
    cur = conn.cursor()
    cur.execute(_TABLES_SQL)
    rows = cur.fetchall()
    result = []
    for schema, table in rows:
        if include_schemas and schema not in include_schemas:
            continue
        fqn = f"{schema}.{table}"
        if fqn in exclude_tables or table in exclude_tables:
            continue
        result.append((schema, table))
    return result


def introspect_table(conn, schema: str, table: str) -> TableDef:
    """Return full TableDef for one MSSQL table."""
    cur = conn.cursor()
    td = TableDef(schema=schema, name=table)

    # --- columns ---
    cur.execute(_COLUMNS_SQL, (schema, table))
    for row in cur.fetchall():
        (col_name, ordinal, data_type, char_max, num_prec, num_scale,
         is_nullable, is_identity, col_default, computed_def) = row
        td.columns.append(ColumnDef(
            name=col_name,
            ordinal=ordinal,
            mssql_type=data_type.lower(),
            char_max_length=char_max,
            numeric_precision=num_prec,
            numeric_scale=num_scale,
            is_nullable=(is_nullable == "YES"),
            is_identity=bool(is_identity),
            default_expr=col_default,
            computed_def=computed_def,
        ))

    # --- primary keys ---
    cur.execute(_PK_SQL, (schema, table))
    td.primary_keys = [r[0] for r in cur.fetchall()]

    # --- unique constraints ---
    uc_map: dict[str, list[str]] = {}
    cur.execute(_UNIQUE_SQL, (schema, table))
    for cname, col in cur.fetchall():
        uc_map.setdefault(cname, []).append(col)
    td.unique_constraints = list(uc_map.values())

    # --- foreign keys ---
    cur.execute(_FK_SQL, (schema, table))
    fk_map: dict[str, dict] = {}
    for row in cur.fetchall():
        (fk_name, src_schema, src_table, src_col,
         ref_schema, ref_table, ref_col, ordinal, on_delete, on_update) = row
        if fk_name not in fk_map:
            fk_map[fk_name] = {
                "name": fk_name,
                "src_cols": [],
                "ref_schema": ref_schema,
                "ref_table": ref_table,
                "ref_cols": [],
                "on_delete": on_delete,
                "on_update": on_update,
            }
        fk_map[fk_name]["src_cols"].append(src_col)
        fk_map[fk_name]["ref_cols"].append(ref_col)
    td.foreign_keys = list(fk_map.values())

    # --- non-PK indexes ---
    fqn = f"{schema}.{table}"
    cur.execute(_INDEX_SQL, (fqn,))
    for idx_name, is_unique, type_desc, cols_str in cur.fetchall():
        td.indexes.append({
            "name": idx_name,
            "is_unique": bool(is_unique),
            "type": type_desc,
            "columns": cols_str.split(",") if cols_str else [],
        })

    # --- check constraints ---
    cur.execute(_CHECK_SQL, (fqn,))
    for chk_name, chk_def in cur.fetchall():
        td.check_constraints.append({"name": chk_name, "definition": chk_def})

    return td
