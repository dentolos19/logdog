from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from parsers.orchestrator import _parse_and_merge
from parsers.contracts import ClassificationResult, ColumnDefinition, StructuralClass
from parsers.deterministic import JsonLinesPipeline
from parsers.normalization import coerce_scalar, sanitize_db_value, sanitize_identifier, unique_identifier
from parsers.preprocessor import FileInput, LogPreprocessorService
from parsers.schema_cache import SchemaCache
from parsers.unified.pipeline import UnifiedPipeline


SAMPLES_DIR = Path(__file__).resolve().parents[3] / "samples" / "test"
ROOT_SAMPLES_DIR = Path(__file__).resolve().parents[3] / "samples"


def _parse_sample(filename: str):
    content = (SAMPLES_DIR / filename).read_text(encoding="utf-8")
    file_input = FileInput(file_id=filename, filename=filename, content=content)
    preprocessor = LogPreprocessorService(
        use_llm=False,
        profile_name="default",
        schema_cache=SchemaCache(use_persistence=False),
    )
    classification = preprocessor.classify([file_input])
    result = _parse_and_merge([file_input], classification)
    return result


def _logical_rows(result, logical_name: str):
    metadata = result.diagnostics.get("parsers", {})
    for _, parser_diag in metadata.items():
        table_metadata = parser_diag.get("table_metadata", {})
        for table_name, table_info in table_metadata.items():
            if table_info.get("logical_name") == logical_name:
                return result.records.get(table_name, [])
    return []


def test_tsv_sample_parses_clean_structured_rows() -> None:
    result = _parse_sample("2-original.tsv")

    assert result.parser_key == "csv"
    rows = list(result.records.values())[0]
    assert len(rows) == 4

    first = rows[0]
    expected_fields = {
        "sample_ts",
        "tool_id",
        "lot_id",
        "wafer_id",
        "site",
        "cd_nm",
        "oxide_thickness_nm",
        "overlay_nm",
        "defect_count",
        "result",
    }
    assert expected_fields.issubset(first.keys())
    assert first["sample_ts"] != "sample_ts"
    assert all(row.get("sample_ts") for row in rows)
    assert all(row.get("tool_id") == "MET-01" for row in rows)

    parser_diag = result.diagnostics["parsers"]["csv"]
    assert parser_diag["quality_gate_failed"] is False

    table_name = list(result.records.keys())[0]
    null_ratios = parser_diag["per_column_null_ratios"][table_name]
    for field in expected_fields:
        assert null_ratios.get(field, 1.0) <= 0.25


def test_json_sample_produces_parent_and_child_tables() -> None:
    result = _parse_sample("1-original.json")

    assert result.parser_key == "json_lines"

    run_rows = _logical_rows(result, "run")
    wafer_rows = _logical_rows(result, "wafers")
    step_rows = _logical_rows(result, "steps")
    alarm_rows = _logical_rows(result, "alarms")

    assert len(run_rows) == 1
    assert len(wafer_rows) == 3
    assert len(step_rows) == 6
    assert len(alarm_rows) == 1

    run = run_rows[0]
    assert run["tool_id"] == "CVD-02"
    assert run["run_id"] == "RUN-20260417-CVD-002"
    assert run["wafer_count"] == 3
    assert run["step_count"] == 6
    assert run["alarm_count"] == 1
    assert run["result_status"] == "COMPLETE_WITH_WARNING"
    assert run["film_uniformity_pct"] == 2.8
    assert run["avg_final_thickness_nm"] == 398.6
    assert run["released_to_metrology"] is True

    parser_diag = result.diagnostics["parsers"]["json_lines"]
    assert parser_diag["quality_gate_failed"] is False
    relationships = parser_diag.get("relationships", [])
    assert relationships


def test_xml_sample_produces_normalized_recipe_tables() -> None:
    result = _parse_sample("3-original.xml")

    assert result.parser_key == "xml"

    recipe_rows = _logical_rows(result, "recipe")
    step_rows = _logical_rows(result, "recipe_steps")
    setpoint_rows = _logical_rows(result, "recipe_setpoints")
    interlock_rows = _logical_rows(result, "recipe_interlocks")
    tolerance_rows = _logical_rows(result, "recipe_tolerances")

    assert len(recipe_rows) == 1
    assert len(step_rows) == 5
    assert len(setpoint_rows) >= 8
    assert len(interlock_rows) == 2
    assert len(tolerance_rows) == 2

    step_1 = next(row for row in step_rows if row["step_seq"] == 1)
    assert step_1["step_name"] == "PUMPDOWN"

    pressure_setpoint = next(
        row for row in setpoint_rows if row["step_seq"] == 1 and row["setpoint_name"] == "target_pressure_mTorr"
    )
    assert pressure_setpoint["setpoint_value"] == 95

    step_1_interlocks = {row["interlock_name"] for row in interlock_rows if row["step_seq"] == 1}
    assert step_1_interlocks == {"door_closed", "helium_backside_ok"}

    etch_tolerances = [row for row in tolerance_rows if row["step_seq"] == 3]
    assert {row["tolerance_name"] for row in etch_tolerances} == {"pressure_mTorr", "electrode_temp_C"}

    serialized_values = " ".join(
        str(value) for rows in result.records.values() for row in rows for value in row.values()
    )
    assert "<?xml" not in serialized_values
    assert "</step>" not in serialized_values
    assert "</recipe>" not in serialized_values

    parser_diag = result.diagnostics["parsers"]["xml"]
    assert parser_diag["quality_gate_failed"] is False


def test_json_lines_parser_falls_back_to_text_rows() -> None:
    content = (ROOT_SAMPLES_DIR / "alarm_events.log").read_text(encoding="utf-8")

    tables, warnings = JsonLinesPipeline()._parse_file(content, "alarm_events.log")

    assert tables
    assert len(tables[0].rows) == 8
    assert tables[0].rows[0]["message"].startswith("[2026-04-17")
    assert any("line-oriented text" in warning for warning in warnings)


def test_unified_pipeline_extracts_hex_dump_payload() -> None:
    content = (ROOT_SAMPLES_DIR / "controller_dump.hex").read_text(encoding="utf-8")
    pipeline = UnifiedPipeline()
    pipeline.schema_inferer = SimpleNamespace(
        infer=lambda **kwargs: SimpleNamespace(
            columns=[
                ColumnDefinition(name="vac_fault", sql_type="INTEGER"),
                ColumnDefinition(name="p", sql_type="INTEGER"),
                ColumnDefinition(name="t", sql_type="REAL"),
                ColumnDefinition(name="rf", sql_type="INTEGER"),
            ],
            null_rates={},
            warnings=[],
            confidence=1.0,
        )
    )
    classification = ClassificationResult(
        dominant_format="plain_text",
        structural_class=StructuralClass.UNSTRUCTURED,
        selected_parser_key="unified",
        file_classifications=[],
    )

    parsed = pipeline._parse_single_file(
        FileInput(file_id="controller_dump", filename="controller_dump.hex", content=content),
        classification,
    )

    assert parsed is not None
    assert any(row.get("p") == 14 for row in parsed.rows)


def test_scalar_and_identifier_normalization_helpers() -> None:
    long_name_1 = "http_schemas_openxmlformats_org_officedocument_2006_extended_properties_application"
    long_name_2 = "http_schemas_openxmlformats_org_officedocument_2006_extended_properties_appversion"

    sanitized_1 = sanitize_identifier(long_name_1)
    sanitized_2 = sanitize_identifier(long_name_2)

    assert len(sanitized_1) <= 63
    assert len(sanitized_2) <= 63
    assert sanitized_1 != sanitized_2
    assert unique_identifier("name", {"name"}) == "name_2"
    assert coerce_scalar("14|") == 14
    assert coerce_scalar("146.0|") == 146.0
    assert sanitize_db_value({"raw": "a\x00b", "items": ["c\x00d"]}) == {"raw": "ab", "items": ["cd"]}
