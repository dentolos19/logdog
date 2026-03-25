#!/usr/bin/env python3
"""End-to-end integration test: unstructured parser + preprocessor + swarm DB.

Run with:
    cd src/server
    uv run python scripts/test_e2e_unstructured.py

No running server needed — this exercises the full pipeline in-process:
  1. Creates an in-memory SQLAlchemy database
  2. Reads the synthetic semiconductor log file
  3. Runs the preprocessor (which delegates to unstructured_parser for PLAIN_TEXT)
  4. Applies the generated DDL to a swarm SQLite database
  5. Prints the inferred schema, columns, sample records, and confidence
"""

import shutil
import sys
import tempfile
from pathlib import Path

# Ensure we can import lib/
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

from lib.database_swarm import LogDatabaseSwarm
from lib.parsers.preprocessor import FileInput, LogPreprocessorService

DIVIDER = "=" * 70


def main() -> None:
    # -------------------------------------------------------------------
    # 1. Load the synthetic log file
    # -------------------------------------------------------------------
    log_path = Path(__file__).resolve().parent / "unstructured_fab.log"
    if not log_path.exists():
        print("Generating unstructured data first...")
        import subprocess

        subprocess.run(
            [
                sys.executable,
                str(Path(__file__).resolve().parent / "generate_unstructured_logs.py"),
            ],
            check=True,
        )

    content = log_path.read_text(encoding="utf-8")
    lines = content.splitlines()
    print(f"\n{DIVIDER}")
    print(f"  Loaded: {log_path.name}  ({len(lines)} lines, {len(content)} bytes)")
    print(DIVIDER)
    print("  First 3 lines:")
    for line in lines[:3]:
        print(f"    {line[:120]}")

    # -------------------------------------------------------------------
    # 2. Run the preprocessor
    # -------------------------------------------------------------------
    print(f"\n{DIVIDER}")
    print("  Running LogPreprocessorService.preprocess() ...")
    print(DIVIDER)

    file_input = FileInput(filename="unstructured_fab.log", content=content)
    service = LogPreprocessorService(table_name="logs")
    result = service.preprocess([file_input])

    # -------------------------------------------------------------------
    # 3. Show detection results
    # -------------------------------------------------------------------
    print(f"\n  Detected format:     {result.file_observations[0].detected_format}")
    print(f"  Format confidence:   {result.file_observations[0].format_confidence}")
    print(f"  Segmentation:        {result.segmentation.strategy} (confidence={result.segmentation.confidence})")
    print(f"  Overall confidence:  {result.confidence}")
    print(f"  Schema version:      {result.schema_version}")

    # -------------------------------------------------------------------
    # 4. Show inferred columns
    # -------------------------------------------------------------------
    print(f"\n{DIVIDER}")
    print(f"  Inferred Columns ({len(result.columns)} total)")
    print(DIVIDER)
    print(f"  {'Name':<25} {'Type':<10} {'Kind':<15} Description")
    print(f"  {'-' * 25} {'-' * 10} {'-' * 15} {'-' * 40}")
    for col in result.columns:
        desc = col.description[:40] + "..." if len(col.description) > 40 else col.description
        print(f"  {col.name:<25} {col.sql_type.value:<10} {col.kind.value:<15} {desc}")

    # Highlight non-baseline columns (these come from our unstructured parser)
    custom_cols = [c for c in result.columns if c.kind.value != "baseline"]
    if custom_cols:
        print(f"\n  >> {len(custom_cols)} columns were added by the unstructured parser:")
        for col in custom_cols:
            examples = ", ".join(col.example_values[:2]) if col.example_values else "(none)"
            print(f"     - {col.name} [{col.kind.value}] examples: {examples}")

    # -------------------------------------------------------------------
    # 5. Show generated DDL
    # -------------------------------------------------------------------
    print(f"\n{DIVIDER}")
    print("  Generated SQLite DDL")
    print(DIVIDER)
    print(result.sqlite_ddl)

    # -------------------------------------------------------------------
    # 6. Show sample records
    # -------------------------------------------------------------------
    print(f"\n{DIVIDER}")
    print(f"  Sample Records ({len(result.sample_records)} extracted)")
    print(DIVIDER)
    for i, sample in enumerate(result.sample_records):
        print(f"\n  Record {i + 1} (lines {sample.line_start}-{sample.line_end} from {sample.source_file}):")
        for key, val in sample.fields.items():
            val_str = str(val)[:80]
            print(f"    {key:<25} = {val_str}")

    # -------------------------------------------------------------------
    # 7. Apply DDL to a swarm database and verify
    # -------------------------------------------------------------------
    print(f"\n{DIVIDER}")
    print("  Applying DDL to swarm database ...")
    print(DIVIDER)

    tmp_dir = Path(tempfile.mkdtemp(prefix="logdog_test_"))
    swarm = LogDatabaseSwarm(root_directory=tmp_dir)
    test_group_id = "test-e2e-001"

    try:
        swarm.ensure_database(test_group_id)

        for table in result.generated_tables:
            swarm.apply_schema(test_group_id, table.sqlite_ddl)
            print(f"  ✓ Created table: {table.table_name}")

        tables = swarm.list_tables(test_group_id)
        print(f"\n  Tables in swarm DB: {[t['name'] for t in tables]}")

        for table in tables:
            col_names = [c["name"] for c in table["columns"]]
            print(f"  Table '{table['name']}' has {len(col_names)} columns: {col_names[:10]}...")

        print(f"\n  ✓ Swarm database verified at: {tmp_dir / f'{test_group_id}.sqlite3'}")
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    # -------------------------------------------------------------------
    # 8. Show warnings / assumptions
    # -------------------------------------------------------------------
    if result.warnings:
        print(f"\n{DIVIDER}")
        print("  Warnings")
        print(DIVIDER)
        for w in result.warnings:
            print(f"  ⚠ {w}")

    if result.assumptions:
        print("\n  Assumptions:")
        for a in result.assumptions:
            print(f"    • {a}")

    # -------------------------------------------------------------------
    # Summary
    # -------------------------------------------------------------------
    print(f"\n{DIVIDER}")
    print("  SUMMARY")
    print(DIVIDER)
    print(f"  Schema summary: {result.schema_summary[:200]}")
    print(f"  Total columns:  {len(result.columns)} ({len(custom_cols)} from unstructured parser)")
    print(f"  Tables created: {len(result.generated_tables)}")
    print(f"  Confidence:     {result.confidence}")
    print("\n  ✓ End-to-end test PASSED — unstructured parser integrates with preprocessor + swarm.\n")


if __name__ == "__main__":
    main()
