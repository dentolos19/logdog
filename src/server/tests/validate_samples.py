"""Validate that all sample files parse correctly through the parser registry."""

import sys
from pathlib import Path

# Ensure the server src is on the path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from parsers.preprocessor import FileInput, LogPreprocessorService
from parsers.registry import ParserRegistry


def test_all_samples():
    samples_dir = Path(__file__).resolve().parent.parent.parent.parent / "samples"
    if not samples_dir.exists():
        print(f"Samples directory not found: {samples_dir}")
        return False

    # Discover all samples recursively
    sample_files = sorted(samples_dir.rglob("*"))
    all_passed = True

    for sample_path in sample_files:
        if not sample_path.is_file():
            continue
        # Skip hidden files
        if sample_path.name.startswith("."):
            continue

        filename = str(sample_path.relative_to(samples_dir))
        try:
            content = sample_path.read_text(encoding="utf-8", errors="ignore")
        except Exception as e:
            print(f"FAIL {filename}: could not read ({e})")
            all_passed = False
            continue

        try:
            # Classify the file (always returns ai_universal)
            preprocessor = LogPreprocessorService(table_name="logs", use_llm=False)
            file_input = FileInput(filename=filename, content=content)
            classification = preprocessor.classify([file_input])

            # Route through registry
            ParserRegistry.discover()
            pipeline = ParserRegistry.route("universal_ai")
            result = pipeline.ingest([file_input], classification)

            if not result.table_definitions and not result.records:
                print(
                    f"FAIL {filename:40s} parser={result.parser_key:20s} no tables"
                )
                all_passed = False
                continue

            row_count = sum(len(rows) for rows in result.records.values())
            table_count = len(result.table_definitions)
            all_columns = set()
            for td in result.table_definitions:
                all_columns.update(c.name for c in td.columns)

            status = "OK"
            if not all_columns:
                status = "WARN"
            if row_count == 0:
                status = "WARN"
                if result.confidence == 0.0:
                    status = "FAIL"

            print(
                f"{status}  {filename:50s} parser={result.parser_key:20s} "
                f"tables={table_count} rows={row_count:4d} "
                f"columns={len(all_columns):3d} "
                f"conf={result.confidence:.2f}"
            )
            if status == "FAIL":
                all_passed = False
        except Exception as e:
            print(f"FAIL {filename}: exception: {e}")
            all_passed = False

    return all_passed


if __name__ == "__main__":
    success = test_all_samples()
    print(f"\n{'All tests passed!' if success else 'Some tests failed!'}")
    sys.exit(0 if success else 1)
