from lib.parsers.preprocessor import FileInput
from lib.parsers.registry import _ParserRegistry
from lib.parsers.semiStructured.adapter import SemiStructuredParserPipeline
from lib.parsers.structured.pipeline import StructuredPipeline
from lib.parsers.unstructured.pipeline import UnstructuredPipeline


def _build_registry() -> _ParserRegistry:
    registry = _ParserRegistry()
    registry.register(StructuredPipeline())
    registry.register(SemiStructuredParserPipeline())
    registry.register(UnstructuredPipeline())
    return registry


def test_resolves_structured_for_json_file() -> None:
    registry = _build_registry()
    file_input = FileInput(filename="events.json", content='{"level":"INFO","message":"hello"}\n{"level":"WARN"}')

    grouped, selections, warnings = registry.resolve_for_files([file_input])

    assert "structured" in grouped
    assert selections[0].parser_key == "structured"
    assert not warnings


def test_resolves_structured_for_xml_file() -> None:
    registry = _build_registry()
    file_input = FileInput(
        filename="recipe.xml",
        content="""
<logs>
  <entry level=\"INFO\" source=\"eqp\">start</entry>
  <entry level=\"WARN\" source=\"eqp\">drift</entry>
</logs>
""".strip(),
    )

    grouped, selections, _warnings = registry.resolve_for_files([file_input])

    assert "structured" in grouped
    assert selections[0].parser_key == "structured"


def test_resolves_unstructured_for_binary_like_content() -> None:
    registry = _build_registry()
    file_input = FileInput(
        filename="capture.bin",
        content="""
0000  1f 8b 08 00 00 00 00 00 02 ff 48 65 6c 6c 6f 20
0010  57 6f 72 6c 64 21 0a 00 00 00 00 00 00 00 00 00
""".strip(),
    )

    grouped, selections, _warnings = registry.resolve_for_files([file_input])

    assert "unstructured" in grouped
    assert selections[0].parser_key == "unstructured"


def test_mixed_batch_routes_per_file() -> None:
    registry = _build_registry()
    json_file = FileInput(filename="a.json", content='{"status":"ok"}\n{"status":"warn"}')
    text_file = FileInput(
        filename="runtime.txt", content="Service started\nNotes: uncertain format\nManual intervention"
    )

    grouped, selections, _warnings = registry.resolve_for_files([json_file, text_file])

    assert len(selections) == 2
    assert len(grouped.keys()) >= 2
    assert any(selection.parser_key == "structured" for selection in selections)
    assert any(selection.parser_key in {"semi_structured", "unstructured"} for selection in selections)
