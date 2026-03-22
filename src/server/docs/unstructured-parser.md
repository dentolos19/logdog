# Unstructured Log Parser

> **Package**: `src/server/lib/parsers/unstructured/`
> **Core logic**: `src/server/lib/parsers/unstructured/core.py`
> **Pipeline adapter**: `src/server/lib/parsers/unstructured/pipeline.py`
> **Tests**: `src/server/tests/test_unstructured_parser.py`

## Overview

The unstructured log parser handles log files that the preprocessor detects as `PLAIN_TEXT` or `UNKNOWN` — files that don't match any structured format (JSON Lines, CSV, syslog, Apache/Nginx CLF, logfmt, or key-value). It transforms raw, free-form text into structured columns and sample records that can be stored in a SQLite swarm database.

It is specifically tuned for **semiconductor fabrication** log data (operator notes, equipment alarms, metrology readouts, maintenance records, process logs), but works on any unstructured text.

## Architecture

The unstructured parser is composed of two layers:

1. **`core.py`** — All parsing logic: encoding detection, noise filtering, multi-line clustering, Drain3 template mining, heuristic field extraction, binary decoders, and LLM enrichment helpers.
2. **`pipeline.py`** — The `UnstructuredPipeline` adapter that implements the `ParserPipeline` interface from `lib/parsers/registry.py`, wiring the core helpers into the team's shared parser framework.

### Integration with the Parser Framework

```
File Upload → routes/logs.py or routes/parser.py
  └── LogPreprocessorService.classify()
        └── ClassificationResult { structural_class: UNSTRUCTURED }
              └── ParserRegistry.route("unstructured")
                    └── UnstructuredPipeline.parse()
                          ├── core.filter_noise()
                          ├── core.cluster_multiline()
                          ├── core.mine_templates() [Drain3]
                          ├── core.extract_fields_heuristic()
                          ├── _detect_fixed_width_fields() [optional]
                          ├── _infer_extra_columns() [frequency-based]
                          ├── _llm_enrich_columns() [optional, via lib.ai]
                          ├── contracts.build_ddl()
                          └── ParserPipelineResult
                                └── orchestrator.run_ingestion_job()
                                      └── LogDatabaseSwarm.apply_schema_and_insert()
```

### Key Files

| File | Purpose | Size |
|------|---------|------|
| `lib/parsers/unstructured/core.py` | All parsing logic: encoding, noise, clustering, Drain3, heuristics, binary decoders, LLM | 35KB |
| `lib/parsers/unstructured/pipeline.py` | `UnstructuredPipeline` adapter implementing `ParserPipeline` interface | 14KB |
| `lib/parsers/unstructured/__init__.py` | Re-exports from core + pipeline | 0.4KB |
| `lib/parsers/contracts.py` | Shared types: `ParserPipelineResult`, `ColumnDefinition`, `TableDefinition`, baseline columns | 7KB |
| `lib/parsers/registry.py` | `ParserRegistry` singleton, `ParserPipeline` ABC, score-based routing | 7KB |
| `lib/parsers/orchestrator.py` | Background job: classify → route → parse → persist to swarm DB | 9KB |
| `lib/ai.py` | Centralized OpenRouter LLM client with structured output | 4KB |

## Pipeline Architecture

The parser runs an **eight-stage pipeline**, invoked via `UnstructuredPipeline.parse()`:

### Stage 1 — Encoding Detection & Normalization

**Functions**: `detect_encoding(raw_bytes)`, `decode_content(raw_bytes)`

- Uses **chardet** to detect the encoding of raw bytes (up to the first 8192 bytes).
- Normalizes common aliases (`ascii`, `windows_1252`, `iso_8859_1`, `latin_1`) to `utf-8`.
- Falls back to `utf-8` with `errors="replace"` if detection or decoding fails.

### Stage 2 — Noise Filtering

**Function**: `filter_noise(lines)`

Removes lines that are:
- Empty or only whitespace
- Decorative separators (dashes `---`, equals `===`, hashes `###`, tildes, asterisks — up to 3 chars)
- Binary content (lines where >30% of characters are non-printable), detected by `is_binary_content(line)`

**Regex used**: `NOISE_LINE_RE = r"^[\s\-=*#~]{0,3}$"`

### Stage 3 — Multi-line Record Clustering

**Function**: `cluster_multiline(lines)`

Groups consecutive lines into logical records. A line is considered a **continuation** of the previous record if it matches any of:
- Starts with `\tat ` (Java stack trace)
- Starts with `Caused by:`
- Starts with `... N more`
- Starts with 4+ spaces or a tab followed by a non-space character

**Returns**: A list of `(start_line, end_line, merged_text)` tuples — 1-based line numbers.

### Stage 4 — Log Template Mining (Drain3)

**Function**: `mine_templates(clusters)`

Runs the **Drain3** algorithm over the first line of each cluster to extract parameterized log templates. Drain3 replaces variable tokens with `<*>` wildcards, grouping structurally similar log lines into the same template cluster.

**Drain3 configuration** (tuned for semiconductor logs):
| Parameter | Value | Purpose |
|-----------|-------|---------|
| `drain_sim_th` | 0.4 | Similarity threshold — lower = more aggressive merging |
| `drain_depth` | 5 | Parse tree depth |
| `drain_max_children` | 512 | Max children per internal node (high variance in semiconductor logs) |
| `drain_max_clusters` | 1024 | Max template clusters |

**Masking instructions** — applied before clustering to prevent high-variance tokens from creating cluster explosion:
| Pattern | Mask | Purpose |
|---------|------|---------|
| `\d{4}[-/]\d{2}[-/]\d{2}T\d{2}:\d{2}:\d{2}...` | `TIMESTAMP` | ISO-8601 and common timestamp formats |
| `0x[A-Fa-f0-9]+` | `HEX` | Hex values (addresses, error codes) |
| `\bW\d{2,4}\b` | `WAFER_ID` | Wafer identifiers (W0045, W1234) |
| `\b(?:LOT\|FOUP)[_-]?\w+\b` | `LOT_ID` | Lot and FOUP identifiers |
| `\bRF_\d+\b` | `RF_CODE` | RF error codes |
| `\b\d+\.\d+(?:[eE][+-]?\d+)?\b` | `NUM` | Floating-point numbers |
| `\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b` | `IP` | IP addresses |
| UUID pattern | `UUID` | UUIDs |

Each record gets two fields from this stage:
- `template` — the mined template string (e.g., `"<*> INFO [<*>] Wafer <*> loaded into chamber."`)
- `template_cluster_id` — an MD5 hash (first 12 hex chars) of the template, used as a grouping key

### Stage 5 — Semantic Field Extraction (Heuristic Regex)

**Function**: `extract_fields_heuristic(text)`

Applies domain-specific regex patterns to extract structured fields from each cluster's text.

#### 5a. Timestamp Extraction
Matches ISO-8601, Date-Month-Year, and Syslog-style timestamps.

#### 5b. Log Level Detection
Matches: `TRACE`, `DEBUG`, `INFO`, `WARN`/`WARNING`, `ERROR`, `FATAL`, `CRITICAL`, `NOTICE`, `ALERT`, `EMERG`/`EMERGENCY`

#### 5c. Semiconductor-Domain Identifiers
| Regex | Matches | Field(s) |
|-------|---------|----------|
| `WAFER_ID_RE` | `W0045`, `LOT-A2024`, `FOUP-X012` | `wafer_id`, `wafer` |
| `TOOL_ID_RE` | `TOOL-CMP-04`, `EQP-CVD-01`, `CHAMBER-PVD-01` | `tool_id`, `tool` |
| `RECIPE_RE` | `RCP-OX-THIN`, `RECIPE-NITRIDE-200` | `recipe_id`, `recipe` |
| `STEP_RE` | `PRE_CLEAN`, `DEPOSITION`, `ETCH` (excludes gas names) | `process_step` |

#### 5d. Measurement Extraction
Matches `key=value` patterns for 25+ semiconductor measurement fields. Values are **unit-stripped** so they can be stored as `REAL` type.

#### 5e. Generic Key-Value Extraction
Matches `key=value` or `key: value` patterns with safeguards against stop words and value clobbering.

#### 5f. Fixed-Width Field Detection (New)
**Function**: `_detect_fixed_width_fields(lines)` in `pipeline.py`

Detects columnar log formats where fields are at fixed byte offsets (e.g., mainframe-style logs, equipment status dumps). Requires:
- ≥5 lines of similar length (within 10% of median)
- ≥2 multi-space gaps (runs of ≥2 consecutive space columns)
- ≥3 resulting field ranges

When detected, fixed-width fields are overlaid onto the heuristic extraction results.

### Stage 6 — Column Inference

**Function**: `_infer_extra_columns(all_fields)` in `pipeline.py`

After fields are extracted from all clusters, this function decides which fields become table columns:

1. **Semiconductor columns** (`wafer_id`, `tool_id`, `recipe_id`, `process_step`) are always included if they appear in any record.
2. **Template columns** (`template`, `template_cluster_id`) are always included.
3. **Frequency threshold**: Other fields must appear in ≥5% of records (minimum 2 occurrences) to become a column.
4. **Type inference**: Fields in `MEASUREMENT_FIELD_NAMES` or whose example values are all numeric are typed as `REAL`. Everything else defaults to `TEXT`.
5. **Overflow**: Fields below the threshold are stored in the `additional_data` JSON blob column.

### Stage 7 — LLM Enrichment (Optional)

**Function**: `_llm_enrich_columns()` in `pipeline.py`, delegating to `core.call_llm_for_unstructured()`

If `OPENROUTER_API_KEY` is set, sends up to 30 sample lines and the list of already-detected columns to an LLM via the centralized `lib.ai.invoke_structured_openrouter()` helper.

**LLM integration details**:
- Uses **structured output** with the `LlmUnstructuredResponse` Pydantic schema from `lib/ai.py`
- Response schema: `fields` (list of `{name, sql_type, description, example_values}`), `summary`, `event_type_hint`, `warnings`
- New fields from the LLM are added as extra columns
- Column names are sanitized to lowercase snake_case
- Duplicate columns (already detected by heuristics) are skipped
- If the API key is missing or the call fails, enrichment is gracefully skipped with a warning
- Successful LLM enrichment boosts the file-level confidence by +0.10

### Stage 8 — Heartbeat Suppression

**Function**: `_suppress_heartbeats()` in `pipeline.py`

After rows are built, this stage removes high-frequency "heartbeat" rows that carry no actionable information. A template is suppressed if:

1. It accounts for **>40%** of all clusters in the file, AND
2. None of its rows have an actionable log level (`WARN`, `ERROR`, `FATAL`, `CRITICAL`, `ALERT`, `EMERG`), AND
3. None of its rows contain measurement fields

Suppressed rows are counted and reported in the warnings list. This prevents periodic status pings from dominating the parsed output and inflating storage costs.

**Note**: Files with fewer than 5 clusters skip heartbeat suppression entirely to avoid false positives on small files.

## Confidence Scoring

Each row gets a per-record `parse_confidence` score based on extraction quality:

| Factor | Score |
|--------|-------|
| Base score (any parsed record) | +0.40 |
| Timestamp extracted | +0.10 |
| Log level extracted | +0.05 |
| Each semiconductor ID (wafer, tool, recipe, step) | +0.05 each, max +0.15 |
| Each measurement field | +0.05 each, max +0.15 |
| Template mined | +0.05 |
| **Maximum** | **0.95** |

File-level confidence is the average of all row confidences, with a +0.10 boost if LLM enrichment succeeded.

## Binary Content Handling

The parser also handles binary/hex-encoded content via a dedicated decoder pipeline.

### Entry Point: `preprocess_binary_input(raw_bytes)`

Checks if the first 2048 bytes have ≥10% non-printable characters. If yes, routes through the binary decoder pipeline; otherwise, decodes as normal text.

### Binary Decoder Pipeline: `decode_binary_content(raw_bytes)`

Applies four sub-decoders in priority order:

| Decoder | Trigger | Output |
|---------|---------|--------|
| `_decode_zlib` | Zlib magic bytes (`78 9C`, `78 01`, `78 DA`) | Decompressed text lines |
| `_decode_base64_frames` | `BEGIN_B64`/`END_B64` markers | Decoded payload text or hex preview |
| `_decode_hex_telemetry` | Continuous hex character stream (16+ chars) | Sensor readings parsed as packed binary |
| `_extract_cleartext_signals` | Printable ASCII runs (4+ chars) in binary noise | Error codes, warnings, and cleartext signals |

### Hex Dump Processing

If >50% of input lines match hex dump format, the parser extracts the ASCII column and feeds that into the main text pipeline.

## Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `OPENROUTER_API_KEY` | `""` (disabled) | API key for LLM enrichment via OpenRouter |
| `OPENROUTER_MODEL` | `inception/mercury-2` | LLM model for field extraction |

## Testing

### Unit Tests

```bash
cd src/server
uv run pytest tests/test_unstructured_parser.py -v
```

Test coverage includes (67 tests):
- Encoding detection & decoding (UTF-8, invalid bytes)
- Noise filtering (blanks, separators, binary lines)
- Multi-line clustering (stack traces, simple lines)
- Drain3 template mining (wildcard extraction, template merging)
- Heuristic field extraction (timestamps, log levels, wafer IDs, recipes, steps, tools, measurements, key-value pairs, level aliases)
- Column inference (frequency threshold, empty input)
- Full pipeline (`extract_unstructured_columns`) with and without LLM
- Sample extraction
- Binary/hex-dump handling (hex dump ASCII extraction, binary content detection, zlib decompression, base64 frame decoding, hex telemetry parsing, cleartext signal extraction)
- **Pipeline adapter tests** (`UnstructuredPipeline`):
  - `parse()` returns valid `ParserPipelineResult`
  - Semiconductor columns included when detected
  - Measurement columns typed as REAL
  - Overflow fields stored in `additional_data`
  - Confidence varies by extraction quality
  - `supports()` scoring for plain text and binary extensions
  - Empty file handling
  - DDL validity
- **Fixed-width detection** (columnar layout detection, free-form rejection, value extraction)
- **Confidence scoring** (rich vs sparse fields, cap at 0.95)

### End-to-End Test

```bash
cd src/server
uv run python scripts/test_e2e_unstructured.py
```

### Synthetic Data Generator

```bash
cd src/server
uv run python scripts/generate_unstructured_logs.py [-o output.log] [-n 60]
```

## Key Constants

| Constant | Value | Purpose |
|----------|-------|---------|
| `MAX_SAMPLE_LINES` | 30 | Max lines sent to LLM for enrichment |
| `MAX_LINE_LENGTH` | 2000 | Lines are truncated to this length before processing |
| `_MIN_FREQUENCY_FLOOR` | 2 | Minimum occurrences for a field to become a column |
| `_MIN_FREQUENCY_RATIO` | 0.05 | Minimum frequency ratio (5%) for column promotion |
| `SCHEMA_VERSION` | "1.0.0" | Schema version tag for parsed records |
