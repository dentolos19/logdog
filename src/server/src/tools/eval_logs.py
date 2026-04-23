from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from parsers.orchestrator import orchestrate_files, register_pipelines
from parsers.preprocessor import FileInput


@dataclass
class EvalCase:
    name: str
    raw_path: Path
    gold_path: Path


@dataclass
class EvalMetrics:
    precision: float
    recall: float
    f1: float
    predicted_fields: int
    gold_fields: int
    true_positive_fields: int


def discover_cases(input_dir: Path) -> list[EvalCase]:
    cases: list[EvalCase] = []

    for gold_path in sorted(input_dir.rglob("*.gold.json")):
        raw_prefix = gold_path.name[: -len(".gold.json")]
        raw_candidates = [
            candidate
            for candidate in gold_path.parent.glob(f"{raw_prefix}.*")
            if candidate.is_file() and candidate.name != gold_path.name
        ]
        if raw_candidates:
            raw_path = raw_candidates[0]
            cases.append(EvalCase(name=raw_prefix, raw_path=raw_path, gold_path=gold_path))

    for case_dir in sorted(path for path in input_dir.iterdir() if path.is_dir()):
        gold_path = case_dir / "gold.json"
        if not gold_path.exists():
            continue

        raw_candidates = [path for path in case_dir.glob("raw.*") if path.is_file()]
        if raw_candidates:
            raw_path = raw_candidates[0]
            cases.append(EvalCase(name=case_dir.name, raw_path=raw_path, gold_path=gold_path))

    unique: dict[str, EvalCase] = {}
    for case in cases:
        unique[str(case.raw_path)] = case

    return list(unique.values())


def load_gold_records(gold_path: Path) -> list[dict[str, Any]]:
    payload = json.loads(gold_path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return [record for record in payload if isinstance(record, dict)]
    if isinstance(payload, dict):
        if isinstance(payload.get("records"), list):
            return [record for record in payload["records"] if isinstance(record, dict)]
    return []


def run_case(case: EvalCase, use_llm: bool = True) -> tuple[EvalMetrics, dict[str, Any]]:
    raw_text = case.raw_path.read_text(encoding="utf-8", errors="ignore")
    result = orchestrate_files(
        group_id="eval",
        file_inputs=[FileInput(filename=case.raw_path.name, content=raw_text)],
        persist=False,
        use_llm=use_llm,
    )

    predicted_rows: list[dict[str, Any]] = []
    for rows in result.records.values():
        predicted_rows.extend(rows)

    gold_rows = load_gold_records(case.gold_path)
    metrics = compute_field_metrics(predicted_rows, gold_rows)

    details = {
        "case": case.name,
        "parser_key": result.parser_key,
        "warnings": result.warnings,
        "row_counts": result.row_counts,
        "table_count": len(result.table_definitions),
    }
    return metrics, details


def compute_field_metrics(predicted_rows: list[dict[str, Any]], gold_rows: list[dict[str, Any]]) -> EvalMetrics:
    predicted_fields = flatten_field_values(predicted_rows)
    gold_fields = flatten_field_values(gold_rows)

    true_positive = predicted_fields & gold_fields
    precision = len(true_positive) / len(predicted_fields) if predicted_fields else 0.0
    recall = len(true_positive) / len(gold_fields) if gold_fields else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall > 0 else 0.0

    return EvalMetrics(
        precision=precision,
        recall=recall,
        f1=f1,
        predicted_fields=len(predicted_fields),
        gold_fields=len(gold_fields),
        true_positive_fields=len(true_positive),
    )


def flatten_field_values(rows: list[dict[str, Any]]) -> set[tuple[str, str]]:
    flattened: set[tuple[str, str]] = set()
    for row in rows:
        for key, value in row.items():
            flattened.add((str(key), normalize_value(value)))
    return flattened


def normalize_value(value: Any) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=True, sort_keys=True)
    if value is None:
        return ""
    return str(value)


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate log parsing quality using gold structured data.")
    parser.add_argument("--input", required=True, help="Input directory containing raw logs and gold JSON files.")
    parser.add_argument(
        "--no-llm",
        action="store_true",
        help="Disable LLM-assisted parsing while evaluating.",
    )

    args = parser.parse_args()

    input_dir = Path(args.input).expanduser().resolve()
    if not input_dir.exists() or not input_dir.is_dir():
        raise SystemExit(f"Input directory does not exist: {input_dir}")

    register_pipelines()
    cases = discover_cases(input_dir)
    if not cases:
        raise SystemExit(
            "No evaluation cases were found. Expected *.gold.json or case folders with raw.* and gold.json."
        )

    aggregate_precision = 0.0
    aggregate_recall = 0.0
    aggregate_f1 = 0.0

    print(f"Running {len(cases)} evaluation case(s) from {input_dir}")
    for case in cases:
        metrics, details = run_case(case, use_llm=not args.no_llm)
        aggregate_precision += metrics.precision
        aggregate_recall += metrics.recall
        aggregate_f1 += metrics.f1

        print(
            " | ".join(
                [
                    f"case={case.name}",
                    f"precision={metrics.precision:.3f}",
                    f"recall={metrics.recall:.3f}",
                    f"f1={metrics.f1:.3f}",
                    f"parser={details['parser_key']}",
                ]
            )
        )

    count = len(cases)
    print("---")
    print(f"macro_precision={aggregate_precision / count:.3f}")
    print(f"macro_recall={aggregate_recall / count:.3f}")
    print(f"macro_f1={aggregate_f1 / count:.3f}")


if __name__ == "__main__":
    main()
