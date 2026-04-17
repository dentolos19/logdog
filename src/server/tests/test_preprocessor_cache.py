from __future__ import annotations

from parsers.preprocessor import FileInput, LogPreprocessorService, StructuralClass
from parsers.schema_cache import SchemaCache


def test_classify_reuses_adaptive_cache_before_heuristics() -> None:
    schema_cache = SchemaCache(use_persistence=False)
    lines = [
        "2026-03-01T10:00:00 INFO tool=ETCH-01 chamber=A wafer=W1 temp=55.2",
        "2026-03-01T10:00:01 INFO tool=ETCH-01 chamber=A wafer=W1 temp=55.3",
    ]

    fingerprint = LogPreprocessorService._fingerprint(lines)
    schema_cache.put(
        sample_lines=lines,
        format_name="logfmt",
        domain="unknown",
        columns=[{"name": "tool", "sql_type": "TEXT"}],
        extraction_strategy="per_line",
        profile_name="default",
        detected_format="logfmt",
        structural_class=StructuralClass.STRUCTURED.value,
        parser_key="logfmt",
        format_confidence=0.96,
        fingerprint=fingerprint,
    )

    service = LogPreprocessorService(
        use_llm=False,
        profile_name="default",
        schema_cache=schema_cache,
    )
    result = service.classify([FileInput(filename="etch.log", content="\n".join(lines))])

    assert result.file_classifications[0].detected_format == "logfmt"
    assert result.file_classifications[0].format_confidence >= 0.8
    assert result.file_classifications[0].structural_class == StructuralClass.STRUCTURED
    assert result.diagnostics["files"][0]["source"] == "adaptive_cache"


def test_classify_falls_back_to_heuristics_on_cache_miss() -> None:
    schema_cache = SchemaCache(use_persistence=False)
    service = LogPreprocessorService(
        use_llm=False,
        profile_name="default",
        schema_cache=schema_cache,
    )

    csv_content = "timestamp,tool_id,value\n2026-03-01T10:00:00,ETCH-01,12.3\n2026-03-01T10:00:01,ETCH-01,12.5"
    result = service.classify([FileInput(filename="sensor.csv", content=csv_content)])

    assert result.file_classifications[0].detected_format == "csv"
    assert result.diagnostics["files"][0]["source"] == "heuristic"
