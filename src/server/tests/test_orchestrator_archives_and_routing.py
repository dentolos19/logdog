from __future__ import annotations

import gzip
import io
import tarfile
import zipfile

from parsers.contracts import ClassificationResult, FileClassification, ParserPipelineResult, StructuralClass
from parsers.orchestrator import _decode_payload, _parse_and_merge
from parsers.preprocessor import FileInput
from parsers.registry import ParserRegistry


def test_decode_payload_expands_zip_members() -> None:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w") as zf:
        zf.writestr("a.log", "first line\nsecond line")
        zf.writestr("nested/b.log", "hello")

    payload = _decode_payload("bundle.zip", buffer.getvalue())

    assert len(payload) == 2
    assert payload[0][0] == "bundle.zip:a.log"
    assert payload[0][1].startswith("first line")
    assert payload[1][0] == "bundle.zip:nested/b.log"
    assert payload[1][1] == "hello"


def test_decode_payload_expands_gzip_payload() -> None:
    raw = b"alpha\nbeta\n"
    gz_bytes = gzip.compress(raw)

    payload = _decode_payload("trace.log.gz", gz_bytes)

    assert len(payload) == 1
    assert payload[0][0] == "trace.log.gz:trace.log"
    assert payload[0][1] == "alpha\nbeta\n"


def test_decode_payload_expands_tar_members() -> None:
    tar_buffer = io.BytesIO()
    with tarfile.open(fileobj=tar_buffer, mode="w") as tf:
        content = b"ts=1 level=INFO msg=start\n"
        member = tarfile.TarInfo(name="inner/events.log")
        member.size = len(content)
        tf.addfile(member, io.BytesIO(content))

    payload = _decode_payload("archive.tar", tar_buffer.getvalue())

    assert len(payload) == 1
    assert payload[0][0] == "archive.tar:inner/events.log"
    assert "msg=start" in payload[0][1]


def test_parse_and_merge_routes_by_detected_format(monkeypatch) -> None:
    requested_parser_keys: list[str] = []

    class _FakePipeline:
        def __init__(self, parser_key: str, confidence: float) -> None:
            self._parser_key = parser_key
            self._confidence = confidence

        def ingest(self, file_inputs: list[FileInput], classification: ClassificationResult) -> ParserPipelineResult:
            assert classification.selected_parser_key == self._parser_key
            return ParserPipelineResult(
                table_definitions=[],
                records={self._parser_key: [{"file_count": len(file_inputs)}]},
                parser_key=self._parser_key,
                warnings=[],
                confidence=self._confidence,
            )

    parser_map = {
        "json_lines": _FakePipeline("json_lines", 0.9),
        "csv": _FakePipeline("csv", 0.7),
    }

    def _fake_route(parser_key: str):
        requested_parser_keys.append(parser_key)
        return parser_map[parser_key]

    monkeypatch.setattr(ParserRegistry, "route", _fake_route)

    file_inputs = [
        FileInput(file_id="f1", filename="events.jsonl", content='{"ok":true}'),
        FileInput(file_id="f2", filename="sensor.csv", content="ts,value\n1,2"),
    ]
    classification = ClassificationResult(
        dominant_format="unknown",
        structural_class=StructuralClass.STRUCTURED,
        selected_parser_key="mixed",
        file_classifications=[
            FileClassification(
                file_id="f1",
                filename="events.jsonl",
                detected_format="json_lines",
                structural_class=StructuralClass.STRUCTURED,
                format_confidence=0.95,
                line_count=1,
            ),
            FileClassification(
                file_id="f2",
                filename="sensor.csv",
                detected_format="csv",
                structural_class=StructuralClass.STRUCTURED,
                format_confidence=0.9,
                line_count=2,
            ),
        ],
        confidence=0.9,
    )

    result = _parse_and_merge(file_inputs=file_inputs, classification=classification)

    assert requested_parser_keys == ["json_lines", "csv"]
    assert result.records["json_lines"][0]["file_count"] == 1
    assert result.records["csv"][0]["file_count"] == 1
    assert result.confidence == 0.8
