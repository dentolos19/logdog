"""Deterministic grouping of rows by their ``extra`` column values.

After parsing, rows may have scattered values in the ``extra`` JSON blob.
This module provides a single function ``group_rows_by_extra`` that reorders
rows so that identical or highly similar ``extra`` values are clustered
together sequentially across the dataset.

Normalization
-------------
- Tokens are extracted from all scalar values reachable inside the extra dict.
- Scalar values are trimmed of leading/trailing whitespace, case-folded,
  and internal repeated whitespace is collapsed.
- Empty strings and ``None`` values are ignored.
- The resulting normalized token tuple is used as the sort key.

Grouping algorithm
------------------
- **Primary sort**: rows with non-empty non-``None`` extra values come first;
  rows with missing/empty/null/``"null"`` extra go to the end.
- **Secondary sort**: by *frequency* of the first normalized token (most
  frequent token → appears earliest), which naturally clusters identical
  values into continuous blocks.
- **Tertiary sort**: by the full token tuple alphabetically (deterministic).
- **Stability tie-breaker**: original row index preserves insertion order
  within identical-token groups.

This gives the desired result:
    [red, blue, white, red, white]  →  [red, red, white, white, blue]
"""

from __future__ import annotations

import json
import re
from collections import Counter
from typing import Any

_RE_COLLAPSE_WS = re.compile(r"\s+")


# ── Public API ────────────────────────────────────────────────────────────


def group_rows_by_extra(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return a new list with rows sorted so that similar ``extra`` values
    are grouped together.

    The original list is **not** modified in-place.  Row order is preserved
    for rows that have no meaningful extra data.
    """
    if not rows:
        return rows

    # Pre-compute tokens and a frequency counter for the first token.
    token_counter: Counter[str] = Counter()
    row_tokens: list[tuple[str, ...]] = []

    for row in rows:
        tokens = extract_extra_tokens(row.get("extra"))
        row_tokens.append(tokens)
        if tokens:
            # Normalize the primary token for frequency counting
            primary = tokens[0]
            token_counter[primary] += 1

    def sort_key(idx: int) -> tuple:
        tokens = row_tokens[idx]
        if not tokens:
            # Rows without extra go to the end.
            # Use a two-part key so that within this group the original
            # row order is preserved.
            return (1, 0, (), idx)

        primary = tokens[0]
        freq = token_counter.get(primary, 0)
        # (0, -freq, tokens, idx) → first sort by has-extra,
        # then highest-frequency primary first, then full token tuple,
        # then original index for stability.
        return (0, -freq, tokens, idx)

    sorted_indices = sorted(range(len(rows)), key=sort_key)
    return [rows[i] for i in sorted_indices]


def parse_extra_value(value: Any) -> Any:
    """Parse a raw ``extra`` column value into a Python object.

    Accepts JSON strings, dicts, lists, scalars, ``None``.
    Returns the parsed value or ``None`` if the value is empty/missing.
    """
    if value is None:
        return None
    if isinstance(value, dict | list):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped or stripped == "null":
            return None
        try:
            return json.loads(stripped)
        except (json.JSONDecodeError, TypeError):
            # Treat unknown string as a leaf value
            return stripped
    return value


def extract_extra_tokens(value: Any) -> tuple[str, ...]:
    """Extract a deterministic tuple of normalized tokens from an ``extra``
    value.

    Walk dict values and list items recursively; ignore keys (we care about
    *values* — the actual data being captured).  Scalars are normalized by
    trimming whitespace, case-folding, and collapsing repeated spaces.
    """
    parsed = parse_extra_value(value)
    if parsed is None:
        return ()

    tokens: list[str] = []
    _collect_tokens(parsed, tokens)
    return tuple(tokens)


# ── Internal helpers ──────────────────────────────────────────────────────


def _collect_tokens(obj: Any, acc: list[str]) -> None:
    """Recursively collect normalized scalar tokens from *obj*."""
    if obj is None:
        return
    if isinstance(obj, dict):
        for v in obj.values():
            _collect_tokens(v, acc)
    elif isinstance(obj, list):
        for item in obj:
            _collect_tokens(item, acc)
    elif isinstance(obj, bool):
        # bool is a subclass of int in Python; handle explicitly first.
        acc.append(_normalize_token(str(obj)))
    elif isinstance(obj, (int, float)):
        acc.append(_normalize_token(str(obj)))
    elif isinstance(obj, str):
        normalized = _normalize_token(obj)
        if normalized:
            acc.append(normalized)
    else:
        # Fallback: convert and normalize.
        text = str(obj)
        normalized = _normalize_token(text)
        if normalized:
            acc.append(normalized)


def _normalize_token(text: str) -> str:
    """Normalize a single token: trim whitespace, case-fold, collapse spaces.

    Returns an empty string if the result would be empty.
    """
    result = text.strip()
    if not result:
        return ""
    result = result.casefold()
    result = _RE_COLLAPSE_WS.sub(" ", result)
    return result
