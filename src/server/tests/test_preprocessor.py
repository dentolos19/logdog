import sqlite3
from unittest.mock import patch

import pytest
from lib.parsers.preprocessor import (
    ColumnKind,
    DetectedFormat,
    FileInput,
    InferredColumn,
    LlmSchemaResponse,
    LogPreprocessorService,
    SegmentationStrategy,
    SqlType,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def service() -> LogPreprocessorService:
    return LogPreprocessorService(table_name="test_entries")


def _mock_llm_schema_response(columns: list[dict] | None = None, summary: str = "Mock summary.") -> LlmSchemaResponse:
    """Build a fake LlmSchemaResponse for mocking."""

    return LlmSchemaResponse(
        columns=[
            {"name": "trace_id", "sql_type": "TEXT", "description": "Distributed trace ID.", "nullable": True},
        ]
        if columns is None
        else columns,
        summary=summary,
        warnings=[],
    )


# ---------------------------------------------------------------------------
# Format Detection
# ---------------------------------------------------------------------------


JSON_LINES_SAMPLE = [
    '{"timestamp": "2025-01-01T00:00:00Z", "level": "INFO", "message": "Server started"}',
    '{"timestamp": "2025-01-01T00:00:01Z", "level": "WARN", "message": "High memory usage"}',
    '{"timestamp": "2025-01-01T00:00:02Z", "level": "ERROR", "message": "Connection timeout"}',
]

SYSLOG_SAMPLE = [
    "Jan  5 14:22:01 webserver01 sshd[12345]: Accepted publickey for admin from 10.0.0.1 port 22 ssh2",
    "Jan  5 14:22:02 webserver01 kernel: [UFW BLOCK] IN=eth0 OUT= MAC=00:11:22:33:44:55",
    "Jan  5 14:22:03 webserver01 cron[9876]: (root) CMD (/usr/bin/certbot renew)",
]

APACHE_ACCESS_LOG = [
    '192.168.1.1 - frank [10/Oct/2024:13:55:36 -0700] "GET /index.html HTTP/1.1" 200 2326',
    '10.0.0.5 - - [10/Oct/2024:13:55:37 -0700] "POST /api/data HTTP/1.1" 201 512',
    '172.16.0.100 - admin [10/Oct/2024:13:55:38 -0700] "DELETE /api/users/42 HTTP/1.1" 204 0',
]

PLAIN_TEXT_SAMPLE = [
    "Application startup complete.",
    "Listening on port 8080.",
    "Ready to accept connections.",
]

JAVA_STACKTRACE_SAMPLE = [
    "2025-01-01 00:00:00.123 ERROR [main] com.example.App - Unhandled exception",
    "java.lang.NullPointerException: null",
    "\tat com.example.App.process(App.java:42)",
    "\tat com.example.App.main(App.java:10)",
    "Caused by: java.io.IOException: Connection refused",
    "\tat java.net.Socket.connect(Socket.java:591)",
    "\tat com.example.Client.open(Client.java:23)",
    "2025-01-01 00:00:01.456 INFO [main] com.example.App - Retrying in 5s",
]

LOGFMT_SAMPLE = [
    'time=2025-06-15T10:30:00Z level=info msg="Request received" method=GET path=/api/users duration=23ms',
    'time=2025-06-15T10:30:01Z level=warn msg="Slow query" query="SELECT * FROM users" duration=1200ms',
    'time=2025-06-15T10:30:02Z level=error msg="Connection refused" host=db.internal port=5432',
]

CSV_SAMPLE = [
    "timestamp,level,source,message",
    "2025-01-01T00:00:00Z,INFO,auth,User logged in",
    "2025-01-01T00:00:01Z,WARN,api,Rate limit approaching",
    "2025-01-01T00:00:02Z,ERROR,db,Query timeout after 30s",
]


class TestFormatDetection:
    def test_json_lines_format_detection(self, service: LogPreprocessorService) -> None:
        detected_format, confidence = service._detect_format(JSON_LINES_SAMPLE)
        assert detected_format == DetectedFormat.JSON_LINES
        assert confidence >= 0.8

    def test_syslog_format_detection(self, service: LogPreprocessorService) -> None:
        detected_format, confidence = service._detect_format(SYSLOG_SAMPLE)
        assert detected_format == DetectedFormat.SYSLOG
        assert confidence >= 0.8

    def test_apache_access_log_detection(self, service: LogPreprocessorService) -> None:
        detected_format, confidence = service._detect_format(APACHE_ACCESS_LOG)
        assert detected_format == DetectedFormat.APACHE_ACCESS
        assert confidence >= 0.8

    def test_plain_text_detection(self, service: LogPreprocessorService) -> None:
        detected_format, confidence = service._detect_format(PLAIN_TEXT_SAMPLE)
        assert detected_format == DetectedFormat.PLAIN_TEXT
        assert confidence > 0.0

    def test_logfmt_format_detection(self, service: LogPreprocessorService) -> None:
        detected_format, confidence = service._detect_format(LOGFMT_SAMPLE)
        assert detected_format == DetectedFormat.LOGFMT
        assert confidence >= 0.8

    def test_csv_format_detection(self, service: LogPreprocessorService) -> None:
        detected_format, confidence = service._detect_format(CSV_SAMPLE)
        assert detected_format == DetectedFormat.CSV
        assert confidence >= 0.5


# ---------------------------------------------------------------------------
# Segmentation
# ---------------------------------------------------------------------------


class TestSegmentation:
    def test_multiline_java_stacktrace_segmentation(self, service: LogPreprocessorService) -> None:
        detected_format, _ = service._detect_format(JAVA_STACKTRACE_SAMPLE)
        segmentation = service._detect_segmentation(JAVA_STACKTRACE_SAMPLE, detected_format)
        assert segmentation.strategy == SegmentationStrategy.PER_MULTILINE_CLUSTER

    def test_per_line_for_json(self, service: LogPreprocessorService) -> None:
        segmentation = service._detect_segmentation(JSON_LINES_SAMPLE, DetectedFormat.JSON_LINES)
        assert segmentation.strategy == SegmentationStrategy.PER_LINE

    def test_per_file_for_short_plain_text(self, service: LogPreprocessorService) -> None:
        short_text = ["A single status message."]
        segmentation = service._detect_segmentation(short_text, DetectedFormat.PLAIN_TEXT)
        assert segmentation.strategy == SegmentationStrategy.PER_FILE


# ---------------------------------------------------------------------------
# Baseline Columns
# ---------------------------------------------------------------------------


EXPECTED_BASELINE_NAMES = {
    "id",
    "timestamp",
    "timestamp_raw",
    "source",
    "source_type",
    "log_level",
    "event_type",
    "message",
    "raw_text",
    "record_group_id",
    "line_start",
    "line_end",
    "parse_confidence",
    "schema_version",
    "additional_data",
}


class TestBaselineColumns:
    def test_baseline_columns_always_present(self, service: LogPreprocessorService) -> None:
        """Verify the 15 required baseline columns are always in the output."""

        file_input = FileInput(filename="test.log", content="\n".join(JSON_LINES_SAMPLE))
        result = service.preprocess([file_input])
        output_names = {column.name for column in result.columns}
        assert EXPECTED_BASELINE_NAMES.issubset(output_names)

    def test_baseline_count(self, service: LogPreprocessorService) -> None:
        baseline = service._build_baseline_columns()
        assert len(baseline) == 15


# ---------------------------------------------------------------------------
# DDL Validity
# ---------------------------------------------------------------------------


class TestDdlGeneration:
    def test_sqlite_ddl_valid(self, service: LogPreprocessorService) -> None:
        """Verify the generated DDL executes without errors in an in-memory SQLite database."""

        file_input = FileInput(filename="test.log", content="\n".join(SYSLOG_SAMPLE))
        result = service.preprocess([file_input])

        connection = sqlite3.connect(":memory:")
        try:
            connection.execute(result.sqlite_ddl)
            # Verify the table was actually created.
            cursor = connection.execute(f'PRAGMA table_info("{result.table_name}")')
            columns = cursor.fetchall()
            assert len(columns) > 0
        finally:
            connection.close()

    def test_all_generated_table_ddls_are_valid(self, service: LogPreprocessorService) -> None:
        file_a = FileInput(file_id="file-a", filename="a.log", content="\n".join(SYSLOG_SAMPLE))
        file_b = FileInput(file_id="file-b", filename="b.log", content="\n".join(CSV_SAMPLE))

        result = service.preprocess([file_a, file_b])

        connection = sqlite3.connect(":memory:")
        try:
            for table in result.generated_tables:
                connection.execute(table.sqlite_ddl)

            created_tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
                ).fetchall()
            }

            assert service.table_name in created_tables
            assert f"{service.table_name}_file-a" in created_tables
            assert f"{service.table_name}_file-b" in created_tables
        finally:
            connection.close()

    def test_ddl_contains_id_primary_key(self, service: LogPreprocessorService) -> None:
        columns = [
            InferredColumn(name="id", sql_type=SqlType.INTEGER, nullable=False, kind=ColumnKind.BASELINE),
            InferredColumn(name="message", sql_type=SqlType.TEXT, nullable=True, kind=ColumnKind.BASELINE),
        ]
        ddl = service._generate_ddl("test_table", columns)
        assert "PRIMARY KEY AUTOINCREMENT" in ddl
        assert "NOT NULL" in ddl


# ---------------------------------------------------------------------------
# Multi-File Schema Merge
# ---------------------------------------------------------------------------


class TestSchemaeMerge:
    def test_multiple_files_schema_merge(self, service: LogPreprocessorService) -> None:
        """Two compatible JSON files should yield a union of their keys."""

        file_a = FileInput(
            filename="a.log",
            content='{"host": "web1", "status": 200}\n{"host": "web2", "status": 404}',
        )
        file_b = FileInput(
            filename="b.log",
            content='{"host": "api1", "latency": 120}\n{"host": "api2", "latency": 55}',
        )

        result = service.preprocess([file_a, file_b])
        column_names = {column.name for column in result.columns}

        # Both host (shared), status (file_a only), and latency (file_b only) should be present.
        assert "host" in column_names
        assert "status" in column_names
        assert "latency" in column_names

    def test_single_file_has_sample_records(self, service: LogPreprocessorService) -> None:
        file_input = FileInput(filename="test.log", content="\n".join(APACHE_ACCESS_LOG))
        result = service.preprocess([file_input])
        assert len(result.sample_records) > 0

    def test_generated_tables_include_normalized_and_per_file_tables(self, service: LogPreprocessorService) -> None:
        file_a = FileInput(file_id="file-a", filename="a.log", content="\n".join(JSON_LINES_SAMPLE))
        file_b = FileInput(file_id="file-b", filename="b.log", content="\n".join(CSV_SAMPLE))

        result = service.preprocess([file_a, file_b])

        generated_names = {table.table_name for table in result.generated_tables}

        assert service.table_name in generated_names
        assert f"{service.table_name}_file-a" in generated_names
        assert f"{service.table_name}_file-b" in generated_names

    def test_per_file_tables_use_file_specific_columns(self, service: LogPreprocessorService) -> None:
        file_a = FileInput(
            file_id="json-file",
            filename="a.log",
            content='{"host": "web1", "status": 200}\n{"host": "web2", "status": 404}',
        )
        file_b = FileInput(
            file_id="csv-file",
            filename="b.csv",
            content="timestamp,level,latency\n2025-01-01T00:00:00Z,INFO,120",
        )

        result = service.preprocess([file_a, file_b])

        per_file_tables = {table.file_id: table for table in result.generated_tables if table.file_id is not None}
        json_table_columns = {column.name for column in per_file_tables["json-file"].columns}
        csv_table_columns = {column.name for column in per_file_tables["csv-file"].columns}

        assert "status" in json_table_columns
        assert "latency" not in json_table_columns
        assert "latency" in csv_table_columns
        assert "status" not in csv_table_columns


# ---------------------------------------------------------------------------
# LLM Fallback
# ---------------------------------------------------------------------------


class TestLlmFallback:
    def test_llm_fallback_on_error(self, service: LogPreprocessorService) -> None:
        """When the LLM call raises an error, the result should still be returned with a warning."""

        # Force LLM to be "available" but raise an error.
        service._llm_available = True

        file_input = FileInput(filename="test.log", content="\n".join(JSON_LINES_SAMPLE))

        with patch.object(service, "_call_llm_for_schema", side_effect=RuntimeError("API unavailable")):
            result = service.preprocess([file_input])

        assert any("LLM enrichment failed" in warning for warning in result.warnings)
        # Should still have baseline columns.
        output_names = {column.name for column in result.columns}
        assert EXPECTED_BASELINE_NAMES.issubset(output_names)

    @patch("lib.parsers.preprocessor.OPENROUTER_API_KEY", "")
    def test_no_api_key_warning(self) -> None:
        """When OPENROUTER_API_KEY is empty, a warning should indicate LLM was skipped."""

        service = LogPreprocessorService()
        file_input = FileInput(filename="test.log", content="\n".join(PLAIN_TEXT_SAMPLE))
        result = service.preprocess([file_input])
        assert any("OPENROUTER_API_KEY" in warning for warning in result.warnings)


# ---------------------------------------------------------------------------
# Edge Cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_empty_file_produces_warning(self, service: LogPreprocessorService) -> None:
        file_input = FileInput(filename="empty.log", content="")
        result = service.preprocess([file_input])
        assert any("empty" in warning.lower() for warning in result.warnings)

    def test_result_schema_version(self, service: LogPreprocessorService) -> None:
        file_input = FileInput(filename="test.log", content="\n".join(JSON_LINES_SAMPLE))
        result = service.preprocess([file_input])
        assert result.schema_version == "1.0.0"

    def test_confidence_in_valid_range(self, service: LogPreprocessorService) -> None:
        file_input = FileInput(filename="test.log", content="\n".join(SYSLOG_SAMPLE))
        result = service.preprocess([file_input])
        assert 0.0 <= result.confidence <= 1.0
