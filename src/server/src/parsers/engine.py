from __future__ import annotations

import csv
import io
import json
import logging
import re
import xml.etree.ElementTree as ET
from typing import Any

from parsers.contracts import (
    BASELINE_COLUMNS,
    BASELINE_COLUMN_NAMES,
    AiExtractionDiagnostics,
    AiSchemaPlan,
    ClassificationResult,
    ColumnDefinition,
    ParserPipelineResult,
    ParserSupportRequest,
    ParserSupportResult,
    TableDefinition,
    build_ddl,
    make_display_name,
    make_megabase_table_name,
)
from parsers.normalization import (
    coerce_scalar,
    infer_log_level,
    infer_sql_type,
    normalize_iso_timestamp,
    sanitize_identifier,
    unique_identifier,
)
from parsers.preprocessor import FileInput
from parsers.registry import ParserPipeline

logger = logging.getLogger(__name__)

# Prompt version for AI parser diagnostics
AI_PARSER_PROMPT_VERSION = "2.0.0"

# ── Generic line chunking (no format-specific patterns) ─────────────────

MAX_CHUNK_CHARS = 8000
MAX_CHUNK_LINES = 500


def chunk_content(content: str, filename: str = "") -> list[dict[str, Any]]:
    """Split log content into generic chunks.

    Groups lines by simple heuristics (blank-line separators, size limits),
    without relying on any format-specific timestamp or header patterns.
    Each chunk preserves source-filename, start/end line numbers, and raw text.
    """
    lines = content.splitlines()
    if not lines:
        return []

    chunks: list[dict[str, Any]] = []
    current_lines: list[str] = []
    current_size = 0

    for idx, line in enumerate(lines):
        line_len = len(line) + 1  # +1 for newline

        # Start a new chunk on blank-line boundaries or size thresholds
        is_blank = not line.strip()

        if is_blank and current_lines and current_size > 0:
            # Blank line marks a boundary — flush current chunk
            chunks.append(_make_chunk(current_lines, filename, chunks))
            current_lines = []
            current_size = 0
            continue

        # Skip purely blank lines
        if is_blank:
            continue

        # If adding this line exceeds limits, flush first
        if (current_lines and current_size + line_len > MAX_CHUNK_CHARS) or len(current_lines) >= MAX_CHUNK_LINES:
            chunks.append(_make_chunk(current_lines, filename, chunks))
            current_lines = []
            current_size = 0

        current_lines.append(line)
        current_size += line_len

    if current_lines:
        chunks.append(_make_chunk(current_lines, filename, chunks))

    return chunks


def _make_chunk(
    lines: list[str],
    filename: str,
    existing_chunks: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build a chunk dict from a group of lines."""
    prev_end = existing_chunks[-1]["end_line"] if existing_chunks else 0
    return {
        "source": filename,
        "start_line": prev_end + 1,
        "end_line": prev_end + len(lines),
        "raw_text": "\n".join(lines),
    }


# ── Generic record normalization (CSV-aware) ───────────────────────────

CSV_DELIMITERS = ",\t|;"


def sniff_is_csv(content: str) -> tuple[csv.Dialect, bool] | None:
    """Try to detect if *content* is delimiter-separated with quoting.

    Returns *(dialect, has_header)* or *None* if not sniffable.
    """
    try:
        dialect = csv.Sniffer().sniff(content[:8192], delimiters=CSV_DELIMITERS)
        has_header = csv.Sniffer().has_header(content[:4096])
        return dialect, has_header
    except csv.Error:
        return None


def normalize_records(
    content: str,
    filename: str = "",
) -> list[dict[str, Any]]:
    """Convert raw file content into logical records.

    For delimiter-separated content (CSV) it uses ``csv.DictReader`` so
    quoted multiline cells are preserved correctly.  For well-formed XML
    documents it uses DOM-based extraction to emit one record per logical
    row element.  For everything else it falls back to physical-line splitting.

    Returns a list of records.  Each record has at minimum:
        ``source``, ``record_index``, ``raw``, ``message``.
    """
    if not content.strip():
        return []

    sniffed = sniff_is_csv(content)

    if sniffed:
        dialect, has_header = sniffed
        reader = csv.DictReader(io.StringIO(content), dialect=dialect)
        records: list[dict[str, Any]] = []
        for idx, row in enumerate(reader):
            if not any(cell and cell.strip() for cell in row.values()):
                continue
            record = {
                "source": filename,
                "record_index": idx,
            }
            for k, v in row.items():
                key = k.strip() if k else f"field_{idx}"
                record[key] = v
            record["raw"] = _reconstruct_csv_row(dialect, row)
            msg = record.get("message") or record.get("Message") or record.get("msg") or ""
            record["message"] = str(msg).strip() if msg else ""
            records.append(record)
        return records

    # Try XML
    xml_records = _normalize_xml_records(content, filename)
    if xml_records is not None:
        return xml_records

    # Fallback: line-based
    records = []
    for idx, line in enumerate(content.splitlines()):
        stripped = line.strip()
        if stripped:
            records.append({
                "source": filename,
                "record_index": idx,
                "raw": line,
                "message": stripped,
            })
    return records


def _reconstruct_csv_row(dialect: csv.Dialect, row: dict[str, str]) -> str:
    """Reconstruct a raw CSV line from a dict row."""
    fieldnames = list(row.keys())
    try:
        import io as _io
        buf = _io.StringIO()
        writer = csv.writer(buf, dialect=dialect)
        writer.writerow([row.get(f, "") for f in fieldnames])
        return buf.getvalue().rstrip("\r\n")
    except Exception:
        return str(row)


# ── XML record normalization ────────────────────────────────────────────

_XML_ROW_GRAIN_MIN_COUNT = 2
"""Minimum number of repeated siblings to be considered a row grain."""


def _normalize_xml_records(content: str, filename: str) -> list[dict[str, Any]] | None:
    """Parse *content* as XML and emit logical records.

    Returns a list of record dicts, or *None* if *content* is not valid XML.
    Each record corresponds to one instance of the detected row-grain element.
    Singleton context fields (e.g. from a ``<Header>`` section) are promoted
    onto every row.
    """
    stripped = content.strip()
    if not (stripped.startswith("<") and ">" in stripped[:200]):
        return None

    try:
        root = ET.fromstring(stripped)
    except ET.ParseError:
        return None

    # ── 1. Find the row-grain element
    grain_tag, grain_parent = _find_xml_row_grain(root)
    if grain_tag is None:
        # No repeated element found — emit a single flattened record
        return _flatten_xml_document(root, filename)

    # ── 2. Collect context fields: singleton text values from ancestors
    context: dict[str, Any] = {}
    _collect_xml_context_from_root(root, grain_tag, grain_parent, context)

    # ── 3. Build one record per grain instance
    records: list[dict[str, Any]] = []
    for idx, grain_el in enumerate(grain_parent.findall(grain_tag)):
        record: dict[str, Any] = {
            "source": filename,
            "record_index": idx,
            **context,  # Inherited singleton context
        }
        _flatten_xml_element_into(grain_el, record)

        # Build raw and message
        raw_text = ET.tostring(grain_el, encoding="unicode")
        record["raw"] = raw_text
        record["message"] = _xml_element_summary(grain_el)[:500]
        records.append(record)

    return records


def _find_xml_row_grain(
    element: ET.Element,
) -> tuple[str | None, ET.Element | None]:
    """Walk the tree to find repeated sibling elements (the row grain).

    Returns *(tag_name, parent_element_or_None)*.
    """
    # Count tag occurrences among this element's direct children
    child_tags: dict[str, list[ET.Element]] = {}
    for child in element:
        if child.tag not in ("#text", "#comment"):
            child_tags.setdefault(child.tag, []).append(child)

    for tag, children in child_tags.items():
        if len(children) >= _XML_ROW_GRAIN_MIN_COUNT:
            return tag, element

    # Recurse into containers that look like they wrap a repeated element
    for child in element:
        if child.tag not in ("#text", "#comment"):
            result = _find_xml_row_grain(child)
            if result[0] is not None:
                return result

    return None, None


def _collect_xml_context_from_root(
    root: ET.Element,
    grain_tag: str,
    grain_parent: ET.Element,
    context: dict[str, Any],
    seen_tags: set[str] | None = None,
) -> None:
    """Walk the tree from *root* down, collecting singleton scalar values
    that are NOT part of the row-grain subtree.

    This collects ancestor sibling context (e.g. ToolID/MachineID from Header)
    without needing parent references.
    """
    if seen_tags is None:
        seen_tags = set()

    def _walk(
        element: ET.Element,
        inside_grain_container: bool,
    ) -> None:
        for child in element:
            if child.tag in ("#text", "#comment"):
                continue

            # If we're at the grain_parent level, skip the grain container
            if element is grain_parent and child.tag == grain_tag:
                continue

            # If we're inside the grain container but not at the grain level,
            # skip children of the grain parent that are not the grain itself
            # (they are handled separately)
            if inside_grain_container and element.tag == grain_tag:
                # Don't descend into grain instances
                continue

            has_text = child.text is not None and child.text.strip()
            has_element_children = any(c.tag not in ("#text", "#comment") for c in child)

            if has_element_children:
                # Container — recurse
                _walk(child, inside_grain_container)
            elif has_text and child.tag not in seen_tags:
                key = _xml_tag_to_snake(child.tag)
                context[key] = child.text.strip()
                seen_tags.add(child.tag)

            # Collect child attributes
            for attr_name, attr_value in child.attrib.items():
                key = _xml_tag_to_snake(attr_name)
                if key not in context:
                    context[key] = attr_value

        # Collect own attributes (except for the grain parent)
        if element is not grain_parent:
            for attr_name, attr_value in element.attrib.items():
                key = _xml_tag_to_snake(attr_name)
                if key not in context:
                    context[key] = attr_value

    _walk(root, inside_grain_container=False)


def _flatten_xml_element_into(
    element: ET.Element,
    record: dict[str, Any],
) -> None:
    """Extract attributes and scalar children from *element* into *record*."""
    # Attributes
    for attr_name, attr_value in element.attrib.items():
        record[_xml_tag_to_snake(attr_name)] = attr_value

    # Scalar child elements
    for child in element:
        if child.tag in ("#text", "#comment"):
            continue
        has_text = child.text is not None and child.text.strip()
        has_element_children = any(c.tag not in ("#text", "#comment") for c in child)
        if has_text and not has_element_children:
            # Leaf element with text
            key = _xml_tag_to_snake(child.tag)
            record[key] = child.text.strip()
            # Also capture attributes as key_unit
            for attr_name, attr_value in child.attrib.items():
                unit_key = f"{key}_{_xml_tag_to_snake(attr_name)}"
                record[unit_key] = attr_value
        elif has_element_children:
            # Recurse
            _flatten_xml_element_into(child, record)


def _flatten_xml_document(
    root: ET.Element,
    filename: str,
) -> list[dict[str, Any]]:
    """Emit a single flattened record for a document with no row grain."""
    record: dict[str, Any] = {
        "source": filename,
        "record_index": 0,
    }
    _flatten_xml_element_into(root, record)
    raw_text = ET.tostring(root, encoding="unicode")
    record["raw"] = raw_text
    record["message"] = _xml_element_summary(root)[:500]
    return [record]


def _xml_element_summary(element: ET.Element) -> str:
    """Short human-readable summary of an XML element."""
    parts = []
    for attr_name, attr_value in element.attrib.items():
        parts.append(f"{attr_name}={attr_value}")
    for child in element:
        if child.tag not in ("#text", "#comment") and child.text and child.text.strip():
            parts.append(f"{_xml_tag_to_snake(child.tag)}={child.text.strip()}")
    if parts:
        return f"{element.tag}: " + ", ".join(parts)
    return element.tag


def _xml_tag_to_snake(tag: str) -> str:
    """Convert an XML tag name to snake_case.

    Handles acronyms: ``ToolID → tool_id``, ``RfPower → rf_power``,
    ``TemperatureUnit → temperature_unit``.
    """
    result = ""
    prev_was_lower = False
    for i, char in enumerate(tag):
        if char.isupper():
            if prev_was_lower:
                result += "_"
            elif (
                i > 0
                and i < len(tag) - 1
                and tag[i - 1].isupper()
                and tag[i + 1].islower()
            ):
                # Transition within an acronym (e.g., RFPower → rf_power).
                # Previous was uppercase, next is lowercase → insert underscore.
                # But skip if we already inserted one (handled by prev_was_lower).
                result += "_"
            result += char.lower()
            prev_was_lower = False
        else:
            result += char.lower()
            prev_was_lower = char != "_"
    return result


def _parse_embedded_payload(record: dict[str, Any]) -> dict[str, Any]:
    """Parse the *message* cell of a record if it contains embedded JSON.

    Returns a dict of fields extracted from the JSON payload, or *None*.
    The original message is preserved in ``raw_message``.
    """
    msg = record.get("message", "")
    if not msg or not isinstance(msg, str):
        return None

    stripped = msg.strip()

    # CSV may have already unescaped JSON, or the JSON is stored with CSV
    # double-double-quoting.  Try raw parse first.
    payload = _try_json_parse(stripped)
    if payload is not None:
        return payload

    # If the JSON was CSV-escaped (double-quotes doubled), here it has been
    # read by csv.DictReader which already unescaped "" -> ".  So the JSON
    # in the cell should already be valid JSON.  If it still fails, the cell
    # is not JSON at all.
    return None


def _try_json_parse(text: str) -> dict[str, Any] | None:
    """Try to parse *text* as a JSON object, with some leniency."""
    # Strip outer quotes if they survived CSV unescaping
    t = text.strip()
    if (t.startswith('"') and t.endswith('"')) or (t.startswith("'") and t.endswith("'")):
        t = t[1:-1]
    if not (t.startswith("{") or t.startswith("[")):
        return None
    try:
        data = json.loads(t, strict=False)
    except (json.JSONDecodeError, ValueError):
        # Try finding JSON inside the text
        try:
            start = t.index("{")
            end = t.rindex("}") + 1
            data = json.loads(t[start:end], strict=False)
        except (ValueError, json.JSONDecodeError):
            return None
    if isinstance(data, dict):
        return _flatten_json_object(data, prefix="")
    return None


# ── Shared helpers ──────────────────────────────────────────────────────


def _collect_columns(row: dict[str, Any], columns: dict[str, set[Any]]) -> None:
    """Collect column values for type inference."""
    for key, value in row.items():
        if key not in columns:
            columns[key] = set()
        if value is not None:
            # Convert unhashable types to hashable representation
            if isinstance(value, (dict, list)):
                columns[key].add(json.dumps(value, ensure_ascii=True, default=str))
            else:
                columns[key].add(value)


def _build_columns(
    columns: dict[str, set[Any]],
    rows: list[dict[str, Any]],
) -> list[ColumnDefinition]:
    """Build column definitions from collected data."""
    result = []
    seen_names: set[str] = set()

    for col in BASELINE_COLUMNS:
        safe_name = unique_identifier(sanitize_identifier(col.name), seen_names)
        result.append(ColumnDefinition(
            name=safe_name,
            sql_type=col.sql_type,
            description=col.description,
            nullable=col.nullable,
            primary_key=col.primary_key,
        ))

    for col_name, values in columns.items():
        if col_name in BASELINE_COLUMN_NAMES:
            continue
        safe_name = unique_identifier(sanitize_identifier(col_name), seen_names)
        sql_type = infer_sql_type(list(values))
        result.append(ColumnDefinition(
            name=safe_name,
            sql_type=sql_type,
            nullable=True,
        ))

    return result


def _detect_delimiter(line: str) -> str:
    """Detect CSV delimiter."""
    delimiters = [",", "\t", "|", ";"]
    counts = {d: line.count(d) for d in delimiters}
    return max(counts, key=counts.get) if max(counts.values()) > 0 else ","


# ── Embedded content enrichment (cross-format) ──────────────────────────


def _flatten_json_object(obj: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    """Flatten a JSON object, storing complex types as JSON strings."""
    result: dict[str, Any] = {}
    for key, value in obj.items():
        safe_key = sanitize_identifier(f"{prefix}_{key}" if prefix else key)
        if isinstance(value, (str, int, float, bool)) or value is None:
            result[safe_key] = value
        elif isinstance(value, dict):
            nested = _flatten_json_object(value, prefix=safe_key)
            result[safe_key] = json.dumps(value, ensure_ascii=True)
            result.update(nested)
        elif isinstance(value, list):
            result[safe_key] = json.dumps(value, ensure_ascii=True)
        else:
            result[safe_key] = str(value)
    return result


def _flatten_json(obj: dict, prefix: str = "") -> dict[str, Any]:
    """Flatten a JSON object, converting nested dicts/lists to JSON strings."""
    result = {}
    for key, value in obj.items():
        safe_key = sanitize_identifier(f"{prefix}{key}" if prefix else key)
        if isinstance(value, dict):
            result[safe_key] = json.dumps(value, ensure_ascii=True)
        elif isinstance(value, list):
            if value and isinstance(value[0], dict):
                result[safe_key] = json.dumps(value, ensure_ascii=True)
            else:
                result[safe_key] = ", ".join(str(v) for v in value)
        else:
            result[safe_key] = value
    return result


def _flatten_json_scalars(obj: dict, prefix: str = "") -> dict[str, Any]:
    """Flatten JSON keeping only scalar values."""
    result = {}
    for key, value in obj.items():
        safe_key = sanitize_identifier(f"{prefix}{key}" if prefix else key)
        if isinstance(value, (str, int, float, bool)) or value is None:
            result[safe_key] = value
        elif isinstance(value, dict):
            nested = _flatten_json_scalars(value, prefix=f"{safe_key}_")
            result.update(nested)
        elif isinstance(value, list):
            if value and isinstance(value[0], dict):
                result[safe_key] = json.dumps(value, ensure_ascii=True)
            else:
                result[safe_key] = ", ".join(str(v) for v in value)
    return result


def _extract_child_tables(
    data: dict,
    filename: str,
    child_tables: dict[str, dict[str, Any]],
    parent_id: str = "",
    path: str = "",
) -> None:
    """Extract arrays of objects as child tables."""
    for key, value in data.items():
        if isinstance(value, list) and value and isinstance(value[0], dict):
            table_key = f"{path}_{key}" if path else key
            if table_key not in child_tables:
                child_tables[table_key] = {"rows": [], "columns": {}}

            for idx, item in enumerate(value):
                if isinstance(item, dict):
                    row = _flatten_json_scalars(item, prefix="")
                    row["source"] = filename
                    row["parent_id"] = parent_id
                    row["item_index"] = idx
                    row["raw"] = json.dumps(item)[:2000]
                    row["message"] = json.dumps(item)[:500]
                    row["timestamp"] = row.get("timestamp", "")
                    row["log_level"] = "INFO"
                    child_tables[table_key]["rows"].append(row)
                    _collect_columns(row, child_tables[table_key]["columns"])

            for item in value:
                if isinstance(item, dict):
                    _extract_child_tables(item, filename, child_tables, parent_id, table_key)
        elif isinstance(value, dict):
            _extract_child_tables(value, filename, child_tables, parent_id, f"{path}_{key}" if path else key)


def _parse_embedded_json(cell: str) -> dict[str, Any] | None:
    """Parse a cell containing embedded JSON. Returns extracted fields or None."""
    cell = cell.strip()
    if not cell:
        return None
    if (cell.startswith('"') and cell.endswith('"')) or (cell.startswith("'") and cell.endswith("'")):
        cell = cell[1:-1]
    cell = cell.replace('""', '"')
    cell = cell.strip()
    if not (cell.startswith("{") or cell.startswith("[")):
        return None
    try:
        data = json.loads(cell, strict=False)
    except (json.JSONDecodeError, ValueError):
        try:
            start = cell.index("{")
            end = cell.rindex("}") + 1
            data = json.loads(cell[start:end], strict=False)
        except (ValueError, json.JSONDecodeError):
            return None
    if isinstance(data, dict):
        return _flatten_json_object(data, prefix="")
    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
        return {"json_array": json.dumps(data, ensure_ascii=True)}
    return None


def _parse_embedded_xml(cell: str) -> dict[str, Any] | None:
    """Parse a cell containing embedded XML. Returns extracted fields or None."""
    cell = cell.strip()
    if not cell:
        return None
    if not (cell.startswith("<") and ">" in cell[:200]):
        return None
    try:
        root = ET.fromstring(cell)
    except ET.ParseError:
        return None
    row: dict[str, Any] = {}
    for attr_name, attr_value in root.attrib.items():
        safe_name = sanitize_identifier(attr_name)
        row[safe_name] = attr_value
    if root.text and root.text.strip():
        row[sanitize_identifier(root.tag)] = root.text.strip()
    for child in root:
        if child.tag not in ("#text", "#comment"):
            _extract_xml_element_data(child, row)
    return row if row else None


def _extract_xml_element_data(elem: ET.Element, row: dict[str, Any]) -> None:
    """Extract data from an XML element into a row dict."""
    for attr_name, attr_value in elem.attrib.items():
        safe_name = sanitize_identifier(attr_name)
        row[safe_name] = attr_value
    if elem.text and elem.text.strip():
        row[sanitize_identifier(elem.tag)] = elem.text.strip()
    for child in elem:
        if child.tag not in ("#text", "#comment"):
            _extract_xml_element_data(child, row)


# Lambda lifecycle parses
_LAMBDA_START_RE = re.compile(
    r"START\s+RequestId:\s*(\S+)\s+Version:\s*(\S+)"
)
_LAMBDA_END_RE = re.compile(
    r"END\s+RequestId:\s*(\S+)"
)
_LAMBDA_REPORT_RE = re.compile(
    r"REPORT\s+RequestId:\s*(\S+)\s+"
    r"Duration:\s*([\d.]+)\s*ms\s+"
    r"Billed\s+Duration:\s*([\d.]+)\s*ms\s+"
    r"Memory\s+Size:\s*(\d+)\s+MB\s+"
    r"Max\s+Memory\s+Used:\s*(\d+)\s+MB"
)
_LAMBDA_XRAY_RE = re.compile(
    r"XRAY\s+TraceId:\s*(\S+)\s+SegmentId:\s*(\S+)\s+Sampled:\s*(\S+)"
)


def _parse_lambda_lifecycle(cell: str) -> dict[str, Any] | None:
    """Parse AWS Lambda lifecycle messages. Returns extracted fields or None."""
    cell = cell.strip()
    if not cell:
        return None
    m = _LAMBDA_START_RE.match(cell)
    if m:
        return {"event_type": "START", "request_id": m.group(1), "version": m.group(2), "message": cell[:500]}
    m = _LAMBDA_END_RE.match(cell)
    if m:
        return {"event_type": "END", "request_id": m.group(1), "message": cell[:500]}
    m = _LAMBDA_REPORT_RE.match(cell)
    if m:
        fields: dict[str, Any] = {
            "event_type": "REPORT",
            "request_id": m.group(1),
            "duration_ms": coerce_scalar(m.group(2)),
            "billed_duration_ms": coerce_scalar(m.group(3)),
            "memory_size_mb": coerce_scalar(m.group(4)),
            "max_memory_used_mb": coerce_scalar(m.group(5)),
            "message": cell[:500],
        }
        xray = _LAMBDA_XRAY_RE.search(cell)
        if xray:
            fields["xray_trace_id"] = xray.group(1)
            fields["segment_id"] = xray.group(2)
            fields["sampled"] = xray.group(3).lower() == "true"
        return fields
    m = _LAMBDA_XRAY_RE.match(cell)
    if m:
        return {"xray_trace_id": m.group(1), "segment_id": m.group(2), "sampled": m.group(3).lower() == "true", "message": cell[:500]}
    return None


LOGFMT_RE = re.compile(r'(\w[\w.\-]*)=(?:"([^"]*)"|(\S+))')


def _parse_logfmt_cell(cell: str) -> dict[str, Any] | None:
    """Parse a cell containing logfmt key=value pairs."""
    cell = cell.strip()
    if not cell:
        return None
    # Skip cells that look like XML or HTML
    if cell.startswith("<") and ">" in cell[:100]:
        return None

    # Require that the text is substantially composed of k=v tokens.
    # Reject mixed text where k=v pairs are a minority.
    tokens = cell.replace("=", " ").split()
    if not tokens:
        return None
    pair_count = 0
    for pair in re.findall(r'(\w[\w.\-]*)=(?:"[^"]*"|\S+)', cell):
        # A valid pair has a key (word chars) and a non-empty value
        key, _, val = pair.partition("=")
        if key and val:
            pair_count += 1
    coverage = pair_count / max(len(tokens), 1)
    if coverage < 0.4:
        return None

    pairs = LOGFMT_RE.findall(cell)
    if len(pairs) < 2:
        return None
    fields: dict[str, Any] = {}
    for key, val_quoted, val_unquoted in pairs:
        safe_key = sanitize_identifier(key)
        value = val_quoted if val_quoted else val_unquoted
        if value:
            fields[safe_key] = coerce_scalar(value)
    return fields if fields else None


def _enrich_row_fields(
    row: dict[str, Any],
    candidate_fields: list[str] | None = None,
    extra_texts: list[str] | None = None,
) -> dict[str, int]:
    """Inspect candidate fields in a row for embedded structure and enrich.

    Checks each candidate string field for:
        JSON object, XML fragment, Lambda lifecycle, logfmt key=value pairs.

    Skips enrichment if the row already has XML-derived fields (indicates
    the record was pre-parsed by a structured normalizer).

    Returns enrichment stats dict with keys: enriched_count, lifecycle_count.
    """
    # Skip enrichment for rows that already have structured XML/container fields
    if any(field in row for field in ("temperature", "pressure", "rf_power", "tool_id", "chamber")):
        return {"enriched_count": 0, "lifecycle_count": 0}
    if candidate_fields is None:
        candidate_fields = ["message", "raw"]

    stats: dict[str, int] = {"enriched_count": 0, "lifecycle_count": 0}

    candidates: list[str] = []
    for field in candidate_fields:
        cell = row.get(field)
        if cell and isinstance(cell, str):
            candidates.append(cell)
    if extra_texts:
        candidates.extend(extra_texts)

    for stripped in candidates:
        stripped = stripped.strip()
        if not stripped:
            continue

        enriched = _parse_embedded_json(stripped)
        if enriched:
            _merge_enriched_fields(row, enriched, source_prefix="json")
            stats["enriched_count"] += 1
            continue

        enriched = _parse_embedded_xml(stripped)
        if enriched:
            _merge_enriched_fields(row, enriched, source_prefix="xml")
            stats["enriched_count"] += 1
            continue

        lifecycle = _parse_lambda_lifecycle(stripped)
        if lifecycle:
            row.update(lifecycle)
            stats["lifecycle_count"] += 1
            continue

        logfmt = _parse_logfmt_cell(stripped)
        if logfmt:
            _merge_enriched_fields(row, logfmt, source_prefix="logfmt")
            stats["enriched_count"] += 1

    return stats


def _merge_enriched_fields(
    row: dict[str, Any],
    enriched: dict[str, Any],
    source_prefix: str = "",
) -> None:
    """Merge enriched fields into the row, respecting collision policy.

    Canonical promoted fields (timestamp, message, level, severity) overwrite.
    Other collisions are prefixed with *source_prefix*_ (e.g. json_key).
    """
    had_message = "message" in row and row["message"] is not None
    original_message = row.get("message", "")

    for key, value in enriched.items():
        if key in ("timestamp", "message", "level", "severity"):
            row[key] = value
        elif key in row:
            if source_prefix:
                row[f"{source_prefix}_{key}"] = value
            else:
                row[f"enriched_{key}"] = value
        else:
            row[key] = value

    if "message" in enriched and had_message:
        row["message_raw"] = original_message


# ── Sparsity control ────────────────────────────────────────────────

SPARSITY_THRESHOLD = 0.6  # columns with >60% null are demoted to attributes

COMMON_COLUMNS = frozenset({
    "timestamp", "source", "record_index", "event_type", "message",
    "log_level", "request_id", "service", "function_name",
    "function_request_id", "xray_trace_id", "version", "severity",
    "cold_start", "function_memory_size", "function_arn", "event_code",
    "machine", "warning", "action", "reticle_id",
})


def _add_to_collect(value: Any, col_map: dict[str, set[Any]], key: str) -> None:
    """Add *value* to *col_map[key]* set, converting unhashable types."""
    if value is not None:
        if isinstance(value, (dict, list)):
            col_map.setdefault(key, set()).add(json.dumps(value, ensure_ascii=True, default=str))
        else:
            col_map.setdefault(key, set()).add(value)


def _apply_sparsity_control(rows: list[dict[str, Any]]) -> list[ColumnDefinition]:
    """Demote very sparse columns into an ``attributes`` JSON column.

    Examines all rows and computes null density per column.  Columns with
    density > *SPARSITY_THRESHOLD* (and not in *COMMON_COLUMNS*) are moved
    into the ``attributes`` dict of each row.

    Returns the final list of *ColumnDefinition* (includes ``attributes``).
    """
    if not rows:
        return _build_columns({}, rows)

    # Compute null density
    total = len(rows)
    densities: dict[str, float] = {}
    for key in set().union(*(r.keys() for r in rows)):
        null_count = sum(1 for r in rows if r.get(key) is None or r.get(key) == "")
        densities[key] = null_count / total

    # Baseline columns are always kept
    baseline_names = BASELINE_COLUMN_NAMES | COMMON_COLUMNS

    # Collect kept and demoted columns
    kept_cols: dict[str, set[Any]] = {}
    for row in rows:
        demoted: dict[str, Any] = {}
        for key, value in list(row.items()):
            if key in baseline_names:
                _add_to_collect(value, kept_cols, key)
            elif densities.get(key, 0) > SPARSITY_THRESHOLD:
                demoted[key] = value
                del row[key]
            else:
                _add_to_collect(value, kept_cols, key)
        if demoted:
            existing_attrs = row.get("attributes")
            if existing_attrs and isinstance(existing_attrs, str):
                try:
                    existing = json.loads(existing_attrs)
                except (json.JSONDecodeError, TypeError):
                    existing = {}
            elif existing_attrs and isinstance(existing_attrs, dict):
                existing = existing_attrs
            else:
                existing = {}
            existing.update(demoted)
            row["attributes"] = json.dumps(existing, ensure_ascii=True, default=str)

    # Build column definitions
    columns = _build_columns(kept_cols, rows)

    # Ensure attributes column exists
    attr_names = {c.name for c in columns}
    if "attributes" not in attr_names:
        columns.append(ColumnDefinition(
            name="attributes",
            sql_type="TEXT",
            description="JSON blob for sparse or event-specific fields.",
            nullable=True,
        ))

    return columns


# ── AI / Universal Parser ──────────────────────────────────────────────


class UniversalAIParser(ParserPipeline):
    """AI-driven universal parser for any log format.

    Uses a two-stage pipeline:
        1. **Record normalization:**  content is pre-parsed into logical records
           (CSV-aware for delimited files).
        2. **AI enrichment:**  embedded JSON payloads are extracted from cells,
           then an LLM suggests a compact schema and extracts rows.
    Falls through to *RawIngestFallbackParser* on failure.
    """

    parser_key = "universal_ai"

    def parse(
        self,
        file_inputs: list[FileInput],
        classification: ClassificationResult,
    ) -> ParserPipelineResult:
        table_name = make_megabase_table_name()
        all_rows: list[dict[str, Any]] = []
        all_columns: dict[str, set[Any]] = {}
        warnings: list[str] = []
        diag = AiExtractionDiagnostics(
            model="",
            prompt_version=AI_PARSER_PROMPT_VERSION,
            schema_cache_hit=False,
        )

        if not file_inputs:
            return self._empty_result(table_name, "No file inputs provided.")

        filename = file_inputs[0].filename

        # ── 1. Normalize records (CSV-aware) ────────────────────────
        all_records: list[dict[str, Any]] = []
        for fi in file_inputs:
            records = normalize_records(fi.content, filename=fi.filename)
            for rec in records:
                rec["source"] = fi.filename
            all_records.extend(records)

        if not all_records:
            warnings.append("No records could be extracted from the file.")
            diag.fallback_reason = "no_records"
            return self._empty_result(table_name, "No records extracted.")

        # ── 2. Parse embedded JSON payloads ─────────────────────────
        json_enriched_count = 0
        for record in all_records:
            payload = _parse_embedded_payload(record)
            if payload:
                for k, v in payload.items():
                    if k not in ("raw", "source", "record_index") and k not in record:
                        record[k] = v
                json_enriched_count += 1

        # ── 3. Schema discovery (try AI, fall back to dynamic) ──────
        schema_plan: AiSchemaPlan | None = None
        try:
            from parsers.llm import LlmEngine

            llm = LlmEngine()
            # Build sample records from the first N records (with limited size)
            sample_records = []
            for rec in all_records[:6]:
                sr = {}
                for k, v in rec.items():
                    if isinstance(v, str) and len(v) > 500:
                        sr[k] = v[:500]
                    else:
                        sr[k] = v
                sample_records.append(sr)

            schema_plan = llm.discover_schema_from_records(sample_records, filename=filename)
        except Exception as e:
            logger.debug("AI schema discovery failed: %s", e)

        if schema_plan and schema_plan.confidence >= 0.3:
            diag.schema_confidence = schema_plan.confidence
            diag.model = getattr(llm, "_prompt_version", "") if schema_plan else ""
            columns_plan = schema_plan.columns
        else:
            warnings.append(
                "AI schema discovery returned low confidence or was unavailable; "
                "using dynamic schema from data."
                if schema_plan
                else "AI schema discovery unavailable; using dynamic schema from data."
            )
            diag.fallback_reason = "schema_unavailable"
            columns_plan = []

        # ── 4. Build column definitions ─────────────────────────────
        base_columns = _build_columns(all_columns, all_rows)
        ai_column_defs: list[ColumnDefinition] = []
        seen_names = {c.name for c in base_columns}
        for col in columns_plan:
            safe_name = sanitize_identifier(col.name)
            if safe_name and safe_name not in seen_names:
                valid_sql_types = {"TEXT", "INTEGER", "BIGINT", "FLOAT", "BOOLEAN", "DATETIME"}
                col_def = ColumnDefinition(
                    name=safe_name,
                    sql_type=col.type.upper() if col.type.upper() in valid_sql_types else "TEXT",
                    description=col.description or "",
                    nullable=col.nullable,
                )
                ai_column_defs.append(col_def)
                seen_names.add(safe_name)
        columns = base_columns + ai_column_defs

        # ── 5. AI extraction from records ───────────────────────────
        batch_count = 0
        repair_count = 0
        confidences: list[float] = []

        if columns_plan:
            # Group records into batches for AI extraction
            BATCH_SIZE = 20
            for batch_start in range(0, len(all_records), BATCH_SIZE):
                batch_records = all_records[batch_start:batch_start + BATCH_SIZE]
                batch_count += 1
                try:
                    batch = llm.extract_rows_from_records(
                        batch_records,
                        columns_plan,
                        filename=filename,
                    )
                except Exception as e:
                    logger.debug("AI extraction failed for batch %d: %s", batch_count, e)
                    batch = None

                if batch and batch.rows and batch.confidence >= 0.3:
                    confidences.append(batch.confidence)
                    for row_data in batch.rows:
                        if isinstance(row_data, dict):
                            row_data.setdefault("source", filename)
                            row_data.setdefault("raw", "")
                            row_data.setdefault("message", "")
                            row_data.setdefault("log_level", "INFO")
                            # Embed enriched fields from the input record
                            rec_idx = row_data.get("record_index")
                            if rec_idx is not None:
                                for rec in batch_records:
                                    if rec.get("record_index") == rec_idx:
                                        for k, v in rec.items():
                                            if k not in row_data and k not in (
                                                "raw", "source", "record_index"
                                            ):
                                                row_data[k] = v
                                        break
                            _enrich_row_fields(row_data)
                            all_rows.append(row_data)
                            _collect_columns(row_data, all_columns)
                else:
                    # Fall back: use the normalized records directly
                    repair_count += 1
                    for rec in batch_records:
                        row_data = {
                            "source": rec.get("source", filename),
                            "raw": rec.get("raw", "")[:2000],
                            "message": rec.get("message", "")[:500],
                            "timestamp": rec.get("timestamp", ""),
                            "log_level": infer_log_level(rec.get("message", "")),
                            "record_index": rec.get("record_index"),
                        }
                        # Copy enriched payload fields
                        for k, v in rec.items():
                            if k not in row_data and k not in (
                                "raw", "source", "record_index", "message"
                            ):
                                row_data[k] = v
                        _enrich_row_fields(row_data)
                        all_rows.append(row_data)
                        _collect_columns(row_data, all_columns)
        else:
            # No AI schema — use normalized records directly
            repair_count = 1
            for rec in all_records:
                row_data = {
                    "source": rec.get("source", filename),
                    "raw": rec.get("raw", "")[:2000],
                    "message": rec.get("message", "")[:500],
                    "timestamp": rec.get("timestamp", ""),
                    "log_level": infer_log_level(rec.get("message", "")),
                    "record_index": rec.get("record_index"),
                }
                for k, v in rec.items():
                    if k not in row_data and k not in (
                        "raw", "source", "record_index", "message"
                    ):
                        row_data[k] = v
                _enrich_row_fields(row_data)
                all_rows.append(row_data)
                _collect_columns(row_data, all_columns)

        # ── 6. Apply sparsity control ───────────────────────────────
        columns = _apply_sparsity_control(all_rows)

        diag.batch_count = batch_count
        diag.total_rows = len(all_rows)
        diag.repair_batch_count = repair_count
        diag.average_confidence = round(
            sum(confidences) / len(confidences), 2
        ) if confidences else 0.0
        diag.json_enriched_count = json_enriched_count

        final_confidence = max(diag.average_confidence, 0.3) if all_rows else 0.0
        if not all_rows:
            return ParserPipelineResult(
                table_definitions=[],
                records={},
                parser_key=self.parser_key,
                warnings=warnings,
                confidence=0.0,
                diagnostics=diag.model_dump(),
            )

        table_def = TableDefinition(
            table_name=table_name,
            display_name=make_display_name("ai_universal", None, filename),
            columns=columns,
            ddl=build_ddl(table_name, columns),
        )

        return ParserPipelineResult(
            table_definitions=[table_def],
            records={table_name: all_rows},
            parser_key=self.parser_key,
            warnings=warnings,
            confidence=round(final_confidence, 2),
            diagnostics=diag.model_dump(),
        )

    def supports(self, request: ParserSupportRequest) -> ParserSupportResult:
        return ParserSupportResult(
            parser_key=self.parser_key,
            supported=True,
            score=0.7,
            detected_format="ai_universal",
        )

    def _empty_result(self, table_name: str, reason: str) -> ParserPipelineResult:
        return ParserPipelineResult(
            table_definitions=[],
            records={},
            parser_key=self.parser_key,
            warnings=[reason],
            confidence=0.0,
            diagnostics={"reason": reason, "skip_table": True},
        )


# ── Raw Ingest Fallback Parser ─────────────────────────────────────────


class RawIngestFallbackParser(ParserPipeline):
    """Safety fallback: stores each line as a single row.

    This parser always succeeds and preserves all raw text, ensuring
    uploads never fail completely even when AI parsing is unavailable.
    """

    parser_key = "raw_ingest"

    def parse(
        self,
        file_inputs: list[FileInput],
        classification: ClassificationResult,
    ) -> ParserPipelineResult:
        table_name = make_megabase_table_name()
        all_rows: list[dict[str, Any]] = []
        all_columns: dict[str, set[Any]] = {}

        for file_input in file_inputs:
            lines = [line for line in file_input.content.splitlines() if line.strip()]
            if not lines:
                continue

            sep_lines = self._split_by_separator(lines, file_input.filename)
            for entry in sep_lines:
                raw = entry["raw"]
                row: dict[str, Any] = {
                    "source": file_input.filename,
                    "raw": raw[:2000],
                    "message": raw[:500],
                    "timestamp": entry.get("timestamp", ""),
                    "log_level": infer_log_level(raw),
                    "source_line": entry.get("start_line", 0),
                }

                # Try basic generic extraction
                _enrich_row_fields(row)

                # Try logfmt/kv extraction from raw
                for km, vm, _ in LOGFMT_RE.findall(raw):
                    safe_key = sanitize_identifier(km)
                    row[safe_key] = coerce_scalar(vm)

                # Try ISO timestamp
                ts_match = re.search(
                    r"(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)",
                    raw,
                )
                if ts_match:
                    row["timestamp"] = normalize_iso_timestamp(ts_match.group(1)) or ts_match.group(1)

                all_rows.append(row)
                _collect_columns(row, all_columns)

        columns = _build_columns(all_columns, all_rows)

        if not all_rows:
            return ParserPipelineResult(
                table_definitions=[],
                records={},
                parser_key=self.parser_key,
                confidence=0.0,
                warnings=["No rows could be extracted from the raw ingest fallback."],
                diagnostics={"skip_table": True},
            )

        table_def = TableDefinition(
            table_name=table_name,
            display_name=make_display_name("raw_ingest", None, file_inputs[0].filename if file_inputs else "data"),
            columns=columns,
            ddl=build_ddl(table_name, columns),
        )

        return ParserPipelineResult(
            table_definitions=[table_def],
            records={table_name: all_rows},
            parser_key=self.parser_key,
            confidence=0.3,
            warnings=["Used raw ingest fallback parser — no AI schema was applied."],
            diagnostics={"row_counts": {table_name: len(all_rows)}},
        )

    def supports(self, request: ParserSupportRequest) -> ParserSupportResult:
        return ParserSupportResult(
            parser_key=self.parser_key,
            supported=True,
            score=0.2,
            detected_format="raw",
        )

    @staticmethod
    def _split_by_separator(
        lines: list[str],
        filename: str,
    ) -> list[dict[str, Any]]:
        """Group consecutive lines by blank-line separators, like *chunk_content*."""
        entries: list[dict[str, Any]] = []
        current_lines: list[str] = []
        line_offset = 0

        for idx, line in enumerate(lines):
            if not line.strip():
                if current_lines:
                    entries.append({
                        "raw": "\n".join(current_lines),
                        "start_line": line_offset + 1,
                        "end_line": line_offset + len(current_lines),
                        "source": filename,
                    })
                    current_lines = []
                line_offset = idx + 1
            else:
                current_lines.append(line)

        if current_lines:
            entries.append({
                "raw": "\n".join(current_lines),
                "start_line": line_offset + 1,
                "end_line": line_offset + len(current_lines),
                "source": filename,
            })

        return entries
