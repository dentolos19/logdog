"""Validate that all sample files parse correctly through the parser registry."""

import sys
from pathlib import Path

# Ensure the server src is on the path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from parsers.binary import is_probably_binary, safe_decode_text
from parsers.preprocessor import FileInput, LogPreprocessorService
from parsers.registry import ParserRegistry


def _make_file_input(sample_path: Path, filename: str) -> FileInput:
    """Create a ``FileInput`` from a sample file, handling binary files safely."""
    raw_bytes = sample_path.read_bytes()

    if is_probably_binary(raw_bytes, filename):
        from parsers.binary import extract_printable_text

        return FileInput(
            filename=filename,
            content=extract_printable_text(raw_bytes),
            raw_bytes=raw_bytes,
            is_binary=True,
            byte_length=len(raw_bytes),
        )

    content = safe_decode_text(raw_bytes)
    return FileInput(
        filename=filename,
        content=content,
        raw_bytes=None,
        is_binary=False,
        byte_length=len(raw_bytes),
    )


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
        # Skip gold files
        if sample_path.suffix == ".gold.json":
            continue

        filename = str(sample_path.relative_to(samples_dir))

        try:
            file_input = _make_file_input(sample_path, filename)

            # Classify the file
            preprocessor = LogPreprocessorService(table_name="logs", use_llm=False)
            classification = preprocessor.classify([file_input])

            # Route through registry (the binary parser is auto-discovered)
            ParserRegistry.discover()
            parser_key = classification.selected_parser_key
            pipeline = ParserRegistry.route(parser_key)
            result = pipeline.ingest([file_input], classification)

            if not result.table_definitions and not result.records:
                print(f"FAIL {filename:40s} parser={result.parser_key:20s} no tables")
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
