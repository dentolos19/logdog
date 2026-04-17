from __future__ import annotations

from parsers.preprocessor import DetectedFormat, FileInput, LogPreprocessorService
from parsers.schema_cache import SchemaCache


def _classify_single(content: str, filename: str):
    service = LogPreprocessorService(
        use_llm=False,
        profile_name="default",
        schema_cache=SchemaCache(use_persistence=False),
    )
    return service.classify([FileInput(filename=filename, content=content)])


def test_selects_json_lines_parser_key() -> None:
    content = '{"ts":"2026-03-01T10:00:00Z","level":"INFO","message":"ok"}\n{"ts":"2026-03-01T10:00:01Z","level":"WARN","message":"hot"}'
    result = _classify_single(content, "events.jsonl")
    assert result.dominant_format == DetectedFormat.JSON_LINES.value
    assert result.selected_parser_key == "json_lines"


def test_selects_csv_parser_key() -> None:
    content = "timestamp,tool,value\n2026-03-01T10:00:00,ETCH-01,1.0\n2026-03-01T10:00:01,ETCH-01,1.1"
    result = _classify_single(content, "sensor.csv")
    assert result.dominant_format == DetectedFormat.CSV.value
    assert result.selected_parser_key == "csv"


def test_selects_csv_parser_key_for_tsv() -> None:
    content = "sample_ts\ttool_id\tvalue\n2026-03-01T10:00:00Z\tETCH-01\t1.0\n2026-03-01T10:00:01Z\tETCH-01\t1.1"
    result = _classify_single(content, "sensor.tsv")
    assert result.dominant_format == DetectedFormat.CSV.value
    assert result.selected_parser_key == "csv"


def test_selects_xml_parser_key() -> None:
    content = '<recipe tool="ETCH-01" chamber="A" name="ETCH_OXIDE_V3" version="3.4" author="eng" />'
    result = _classify_single(content, "recipe.xml")
    assert result.dominant_format == DetectedFormat.XML.value
    assert result.selected_parser_key == "xml"


def test_selects_syslog_parser_key() -> None:
    content = "Mar 10 10:15:23 host app[123]: started\nMar 10 10:15:24 host app[123]: warning threshold"
    result = _classify_single(content, "syslog.log")
    assert result.dominant_format == DetectedFormat.SYSLOG.value
    assert result.selected_parser_key == "syslog"


def test_selects_logfmt_parser_key() -> None:
    content = "level=INFO tool=ETCH-01 wafer=W1 msg=ready\nlevel=ERROR tool=ETCH-01 wafer=W1 msg=alarm"
    result = _classify_single(content, "tool.log")
    assert result.dominant_format == DetectedFormat.LOGFMT.value
    assert result.selected_parser_key == "logfmt"


def test_selects_key_value_parser_key() -> None:
    content = "tool:ETCH-01 chamber:A lot:L1 status:running\ntool:ETCH-01 chamber:A lot:L1 status:done"
    result = _classify_single(content, "kv.log")
    assert result.dominant_format == DetectedFormat.KEY_VALUE.value
    assert result.selected_parser_key == "key_value"
