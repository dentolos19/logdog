# Branch: `feature/unstructured-parser-v3`

> **Author**: T-Kishan  
> **Base branch**: `main`  
> **Status**: Ready for review — tests passing, docs added

---

## What This Branch Does

This branch delivers **v3 of the unstructured log parser** — the fallback parser that handles any log file the preprocessor classifies as `PLAIN_TEXT` or `UNKNOWN`. It is specifically tuned for semiconductor fabrication logs (equipment alarms, operator notes, metrology readouts, process logs) but works on any free-form text.

The parser lives at [`src/server/lib/parsers/unstructured/`](src/server/lib/parsers/unstructured/) and integrates with the shared `ParserPipeline` interface used by the structured and semi-structured parsers.

---

## Key Changes in This Branch

### `src/server/lib/parsers/unstructured/pipeline.py` — Major rewrite (+664 lines)

The pipeline adapter was rebuilt from scratch around the `ParserPipeline` interface:

| Feature | Detail |
|---------|--------|
| **Drain3 masking config** | `_build_drain_config()` in `core.py` now applies 8 masking rules (timestamps, hex values, wafer IDs, lot IDs, RF codes, floats, IPs, UUIDs) before clustering — prevents cluster explosion on high-variance tokens |
| **Heartbeat suppression** | `_suppress_heartbeats()` removes templates that account for >40% of all clusters and carry no actionable log level or measurement fields |
| **Frequency-based column inference** | `_infer_extra_columns()` promotes fields to dedicated columns only if they appear in ≥5% of records (min 2 occurrences) — sparse fields go into `additional_data` JSON blob |
| **Fixed-width columnar detection** | `_detect_fixed_width_fields()` detects mainframe-style aligned columns and overlays them onto heuristic extraction |
| **Per-row confidence scoring** | `_compute_row_confidence()` scores each row 0.40–0.95 based on timestamp, log level, semiconductor IDs, measurements, and template presence |
| **LLM enrichment** | `_llm_enrich_columns()` calls `lib.ai.invoke_structured_openrouter()` when `OPENROUTER_API_KEY` is set — adds LLM-discovered columns and boosts file confidence by +0.10 |

### `src/server/lib/parsers/unstructured/core.py` — Drain3 masking added

- `_build_drain_config()` now configures `TemplateMinerConfig` programmatically with semiconductor-specific masking instructions
- `call_llm_for_unstructured()` uses the centralized `lib.ai.invoke_structured_openrouter()` helper (no direct `ChatOpenRouter` instantiation)
- `MEASUREMENT_FIELD_NAMES` includes `target` — single source of truth imported by `pipeline.py`

### `src/server/tests/test_unstructured_parser.py` — 342 lines of new tests (+67 total)

New test coverage added for:
- `UnstructuredPipeline` adapter (parse, supports, semiconductor columns, REAL typing, overflow, confidence, DDL validity, empty file)
- Fixed-width detection (columnar layout detection, free-form rejection, value extraction)
- Confidence scoring (rich vs sparse fields, cap at 0.95)
- Heartbeat suppression (high-frequency templates, actionable level bypass, small file skip)

### `src/server/lib/ai.py` — Minor addition

Added `has_openrouter_api_key()` helper used by the pipeline to gate LLM enrichment.

### New files (untracked, to be committed)

| File | Purpose |
|------|---------|
| [`plans/unstructured-parser-plan.md`](plans/unstructured-parser-plan.md) | Refined completion plan — maps original plan vs. reality, lists remaining P1/P2 work |
| [`src/server/docs/unstructured-parser.md`](src/server/docs/unstructured-parser.md) | Full technical reference for the unstructured parser — architecture, all 8 pipeline stages, confidence scoring, binary handling, test commands |

---

## Architecture Overview

```
File Upload → routes/logs.py or routes/parser.py
  └── LogPreprocessorService.classify()
        └── ClassificationResult { structural_class: UNSTRUCTURED }
              └── ParserRegistry.route("unstructured")
                    └── UnstructuredPipeline.parse()
                          ├── 1. core.filter_noise()
                          ├── 2. core.cluster_multiline()
                          ├── 3. core.mine_templates() [Drain3 + masking]
                          ├── 4. core.extract_fields_heuristic()
                          ├── 5. _infer_extra_columns() [frequency threshold]
                          ├── 6. _llm_enrich_columns() [optional]
                          ├── 7. _suppress_heartbeats() [>40% frequency]
                          └── 8. build_ddl() → ParserPipelineResult
                                └── orchestrator.run_ingestion_job()
                                      └── LogDatabaseSwarm.apply_schema_and_insert()
```

---

## How to Test

```bash
# Unit tests (67 tests)
cd src/server
uv run pytest tests/test_unstructured_parser.py -v

# End-to-end test against synthetic fab logs
uv run python scripts/test_e2e_unstructured.py

# Generate synthetic semiconductor log data
uv run python scripts/generate_unstructured_logs.py -o test.log -n 60
```

---

## What's Still Pending (P2 — Nice to Have)

These are not blockers for merging:

- **Drain3 `FilePersistence`**: Persist template state across sessions so the miner improves over time
- **PDF OCR integration**: Wire `scripts/extract_pdf_logs.py` into the pipeline as a preprocessing step for `.pdf` files
- **Async support**: The pipeline is synchronous; `asyncio.TaskGroup` would help for large batch LLM calls

See [`plans/unstructured-parser-plan.md`](plans/unstructured-parser-plan.md) for the full roadmap.
