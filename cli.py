"""
CLI entry point.

Usage
-----
pgloader-py migrate \\
  --ms-host myserver.database.windows.net \\
  --ms-db   mydb  --ms-user sa --ms-password secret \\
  --pg-host mypg.postgres.database.azure.com \\
  --pg-db   mydb  --pg-user pgadmin --pg-password secret \\
  --workers 4 --chunk-size 50000

All options can also be provided via environment variables or a .env file.
"""

from __future__ import annotations

import logging
import os
import sys
import time
import threading
from typing import Optional

import click
from dotenv import load_dotenv
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table
from rich.text import Text

from .migrate import (
    MssqlConfig,
    PgConfig,
    MigrationConfig,
    MigrationResult,
    run_migration,
)

load_dotenv()
console = Console()


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        level=level,
        handlers=[logging.StreamHandler(sys.stderr)],
    )
    # Quiet noisy third-party loggers
    logging.getLogger("pymssql").setLevel(logging.WARNING)


# ---------------------------------------------------------------------------
# Rich progress integration
# ---------------------------------------------------------------------------

class MigrationProgress:
    """Thread-safe progress tracking for Rich Live display."""

    def __init__(self):
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.fields[phase]}", justify="right"),
            BarColumn(),
            MofNCompleteColumn(),
            TextColumn("·"),
            TimeElapsedColumn(),
            TextColumn("{task.fields[msg]}"),
        )
        self._tasks: dict[str, any] = {}
        self._lock = threading.Lock()

    def __call__(self, phase: str, current: int, total: int, message: str = ""):
        with self._lock:
            if phase not in self._tasks:
                self._tasks[phase] = self.progress.add_task(
                    phase, total=total, phase=phase, msg=""
                )
            task_id = self._tasks[phase]
            self.progress.update(
                task_id,
                completed=current,
                total=max(total, 1),
                msg=message[:80],
            )


# ---------------------------------------------------------------------------
# Result summary
# ---------------------------------------------------------------------------

def _print_summary(result: MigrationResult) -> None:
    console.print()

    # Table of per-table results
    tbl = Table(title="Migration Summary", show_lines=True)
    tbl.add_column("Schema.Table", style="cyan", no_wrap=True)
    tbl.add_column("Rows", justify="right", style="green")
    tbl.add_column("Time (s)", justify="right")
    tbl.add_column("Status", justify="center")

    for tr in sorted(result.table_results, key=lambda r: (r.schema, r.table)):
        status = Text("✓ OK", style="green") if tr.ok else Text("✗ FAIL", style="red")
        tbl.add_row(
            f"{tr.schema}.{tr.table}",
            f"{tr.rows:,}" if tr.ok else "–",
            f"{tr.elapsed:.1f}",
            status,
        )

    console.print(tbl)

    if result.failed_tables:
        console.print()
        for tr in result.failed_tables:
            console.print(
                Panel(
                    tr.error or "(unknown error)",
                    title=f"[red]Error: {tr.schema}.{tr.table}",
                    border_style="red",
                )
            )

    if result.schema_errors:
        console.print()
        console.print("[yellow]Schema / index / FK warnings:[/yellow]")
        for err in result.schema_errors:
            console.print(f"  [yellow]⚠[/yellow] {err}")

    console.print()
    console.print(
        f"[bold]Total:[/bold] {result.rows_total:,} rows  |  "
        f"{len(result.table_results) - len(result.failed_tables)} tables OK  |  "
        f"{len(result.failed_tables)} failed  |  "
        f"elapsed {result.total_elapsed:.1f}s"
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@click.group()
def main():
    """pgloader-py – MSSQL → PostgreSQL migration tool."""


@main.command("migrate")
# MSSQL source
@click.option("--ms-host",     envvar="MS_HOST",     required=True,  help="MSSQL server host")
@click.option("--ms-port",     envvar="MS_PORT",     default=1433,   type=int, show_default=True)
@click.option("--ms-db",       envvar="MS_DATABASE", required=True,  help="MSSQL database name")
@click.option("--ms-user",     envvar="MS_USER",     required=True)
@click.option("--ms-password", envvar="MS_PASSWORD", required=True)
@click.option("--ms-tds",      envvar="MS_TDS",      default="7.4",  show_default=True,
              help="TDS version (7.3 for older SQL Server)")
# PostgreSQL target
@click.option("--pg-host",     envvar="PG_HOST",     required=True,  help="PostgreSQL host")
@click.option("--pg-port",     envvar="PG_PORT",     default=5432,   type=int, show_default=True)
@click.option("--pg-db",       envvar="PG_DATABASE", required=True,  help="PostgreSQL database name")
@click.option("--pg-user",     envvar="PG_USER",     required=True)
@click.option("--pg-password", envvar="PG_PASSWORD", required=True)
@click.option("--pg-sslmode",  envvar="PG_SSLMODE",  default="require", show_default=True,
              help="PostgreSQL sslmode (disable/require/verify-ca/verify-full)")
# Migration options
@click.option("--include-schema", "include_schemas", multiple=True,
              help="Only migrate these schemas (repeatable). Default: all.")
@click.option("--exclude-table",  "exclude_tables",  multiple=True,
              help="Skip these tables, e.g. dbo.StagingLog (repeatable).")
@click.option("--pg-schema",  default=None,
              help="Map all source schemas to this single PG schema.")
@click.option("--workers",    default=4,      type=int,  show_default=True,
              help="Parallel COPY worker threads.")
@click.option("--chunk-size", default=50_000, type=int,  show_default=True,
              help="Rows per COPY batch.")
@click.option("--truncate",   is_flag=True, default=False,
              help="TRUNCATE target tables before COPY.")
@click.option("--drop-tables", is_flag=True, default=False,
              help="DROP target tables before re-creating them.")
@click.option("--verbose", "-v", is_flag=True, default=False)
def migrate(
    ms_host, ms_port, ms_db, ms_user, ms_password, ms_tds,
    pg_host, pg_port, pg_db, pg_user, pg_password, pg_sslmode,
    include_schemas, exclude_tables, pg_schema,
    workers, chunk_size, truncate, drop_tables, verbose,
):
    """Migrate all tables from MSSQL to PostgreSQL using COPY (no INSERT)."""
    _setup_logging(verbose)

    cfg = MigrationConfig(
        mssql=MssqlConfig(
            host=ms_host,
            port=ms_port,
            database=ms_db,
            user=ms_user,
            password=ms_password,
            tds_version=ms_tds,
        ),
        pg=PgConfig(
            host=pg_host,
            port=pg_port,
            database=pg_db,
            user=pg_user,
            password=pg_password,
            sslmode=pg_sslmode,
        ),
        include_schemas=list(include_schemas) or None,
        exclude_tables=list(exclude_tables),
        pg_schema_override=pg_schema,
        chunk_size=chunk_size,
        workers=workers,
        truncate=truncate,
        drop_tables=drop_tables,
    )

    prog = MigrationProgress()

    console.print(
        Panel.fit(
            "[bold cyan]pgloader-py[/bold cyan]  MSSQL → PostgreSQL migration\n"
            f"Source: [green]{ms_user}@{ms_host}/{ms_db}[/green]\n"
            f"Target: [green]{pg_user}@{pg_host}/{pg_db}[/green]\n"
            f"Workers: {workers}  |  Chunk: {chunk_size:,}  |  "
            f"Truncate: {truncate}  |  Drop: {drop_tables}",
            title="Configuration",
        )
    )

    with Live(prog.progress, console=console, refresh_per_second=10):
        result = run_migration(cfg, progress_callback=prog)

    _print_summary(result)
    sys.exit(1 if result.failed_tables else 0)


@main.command("test-connection")
@click.option("--ms-host",     envvar="MS_HOST",     required=True)
@click.option("--ms-port",     envvar="MS_PORT",     default=1433, type=int)
@click.option("--ms-db",       envvar="MS_DATABASE", required=True)
@click.option("--ms-user",     envvar="MS_USER",     required=True)
@click.option("--ms-password", envvar="MS_PASSWORD", required=True)
@click.option("--pg-host",     envvar="PG_HOST",     required=True)
@click.option("--pg-port",     envvar="PG_PORT",     default=5432, type=int)
@click.option("--pg-db",       envvar="PG_DATABASE", required=True)
@click.option("--pg-user",     envvar="PG_USER",     required=True)
@click.option("--pg-password", envvar="PG_PASSWORD", required=True)
@click.option("--pg-sslmode",  envvar="PG_SSLMODE",  default="require")
def test_connection(
    ms_host, ms_port, ms_db, ms_user, ms_password,
    pg_host, pg_port, pg_db, pg_user, pg_password, pg_sslmode,
):
    """Test connectivity to both databases."""
    import pymssql
    import psycopg

    # MSSQL
    console.print("Testing MSSQL connection…", end=" ")
    try:
        c = pymssql.connect(
            server=ms_host, port=ms_port, database=ms_db,
            user=ms_user, password=ms_password, login_timeout=10,
        )
        cur = c.cursor()
        cur.execute("SELECT @@VERSION")
        ver = cur.fetchone()[0].split("\n")[0].strip()
        c.close()
        console.print(f"[green]OK[/green]  {ver}")
    except Exception as e:
        console.print(f"[red]FAIL[/red]  {e}")

    # PostgreSQL
    console.print("Testing PostgreSQL connection…", end=" ")
    try:
        c = psycopg.connect(
            host=pg_host, port=pg_port, dbname=pg_db,
            user=pg_user, password=pg_password, sslmode=pg_sslmode,
            connect_timeout=10,
        )
        cur = c.cursor()
        cur.execute("SELECT version()")
        ver = cur.fetchone()[0].split(",")[0].strip()
        c.close()
        console.print(f"[green]OK[/green]  {ver}")
    except Exception as e:
        console.print(f"[red]FAIL[/red]  {e}")
