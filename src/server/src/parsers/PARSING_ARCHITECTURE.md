# Parser Architecture

## Deterministic-First Routing

The parser system routes structured formats to deterministic parsers before any fallback parsing.

- JSON (`.json`, `.jsonl`, `.ndjson`) -> `json_lines` deterministic parser.
- XML (`.xml`) -> `xml` deterministic parser.
- Delimited files (`.csv`, `.tsv` and content-sniffed comma/tab/pipe/semicolon) -> `csv` deterministic parser.
- Syslog, Apache/Nginx access, logfmt, and key-value formats -> deterministic parsers.
- Unified fallback (`unified`) is used only when format detection is unstructured, malformed, or a deterministic quality gate fails.

## Structured Normalization

### JSON

The deterministic JSON parser performs document-level parsing.

- Root scalar/object values are projected to a parent table.
- Nested arrays of objects are normalized to child tables.
- Parent-child relationships are emitted in parse diagnostics.
- Derived summary fields are generated when possible (for example: `wafer_count`, `step_count`, `alarm_count`, and `result_*` fields in deposition-run payloads).

### XML

The deterministic XML parser performs tree traversal with semantic table projection.

For recipe payloads, normalized tables are emitted for:

- recipe metadata.
- recipe steps.
- recipe setpoints.
- recipe interlocks.
- recipe tolerances.

XML declaration and closing-tag rows are never emitted as records because parsing is element-based rather than line-based.

### Delimited CSV/TSV

Delimited parser behavior:

- Sniffs delimiter from `,`, `\t`, `|`, `;`.
- Detects header rows and avoids emitting headers as data records.
- Preserves empty-string vs null semantics.
- Applies explicit scalar type coercion for integers, floats, booleans, timestamps, and enum-like status fields.

## Null-Ratio Validation and Quality Gates

Deterministic structured outputs are validated before final acceptance.

Quality diagnostics include:

- Per-table row counts.
- Per-column null ratios.
- Per-table null ratios.
- Validation warnings.

Validation checks include:

- Header row emitted as data.
- XML tag-only rows.
- XML declaration-only rows.
- Duplicate `raw/message` masking structured extraction.
- Required-field null ratio thresholds.

If required structured fields exceed quality thresholds (default 30% null ratio), the deterministic parse is marked as failed and a controlled fallback path is used. Fallback confidence is penalized heavily.

## Output Diagnostics Contract

Parse responses include:

- `parser_key`: parser used after routing/fallback.
- `warnings`: parser and validation warnings.
- `diagnostics.table_row_counts`: row counts by table.
- `diagnostics.per_column_null_ratios`: null ratio by column.
- `diagnostics.per_table_null_ratios`: aggregate null ratio by table.
- `diagnostics.relationships`: parent-child relationships for normalized structured tables.
- `diagnostics.validation_warnings`: detailed quality findings.

This contract allows downstream services to distinguish successful structured parsing from traceability-only fallback behavior.
