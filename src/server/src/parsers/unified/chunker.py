from __future__ import annotations

import re
from dataclasses import dataclass, field

SECTION_HEADER_RE = re.compile(r"^(?:---\s+.+\s+---|\[[^\]]+\]|\w.+:)\s*$")
TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}")


@dataclass
class Chunk:
    index: int
    start_line: int
    end_line: int
    lines: list[str] = field(default_factory=list)
    reason: str = "size"


@dataclass
class ChunkingResult:
    chunks: list[Chunk] = field(default_factory=list)
    strategy: str = "fixed"
    warnings: list[str] = field(default_factory=list)


class AdaptiveChunker:
    def __init__(self, target_chunk_size: int = 500, max_chunk_size: int = 2000):
        self.target_chunk_size = target_chunk_size
        self.max_chunk_size = max_chunk_size

    def chunk_lines(self, lines: list[str]) -> ChunkingResult:
        if not lines:
            return ChunkingResult(chunks=[], strategy="empty")

        if len(lines) <= self.target_chunk_size:
            return ChunkingResult(
                chunks=[Chunk(index=0, start_line=1, end_line=len(lines), lines=lines, reason="small_file")],
                strategy="single",
            )

        boundaries = self._detect_boundaries(lines)
        if boundaries:
            chunks = self._chunk_by_boundaries(lines, boundaries)
            return ChunkingResult(chunks=chunks, strategy="boundary_aware")

        chunks = self._chunk_fixed(lines)
        return ChunkingResult(chunks=chunks, strategy="fixed")

    def _detect_boundaries(self, lines: list[str]) -> list[int]:
        boundaries: list[int] = [0]

        for i, line in enumerate(lines):
            if i == 0:
                continue

            stripped = line.strip()
            if not stripped:
                continue

            if SECTION_HEADER_RE.match(stripped):
                boundaries.append(i)
                continue

            if TIMESTAMP_RE.match(stripped):
                prev_line = lines[i - 1].strip() if i > 0 else ""
                if prev_line and not TIMESTAMP_RE.match(prev_line):
                    boundaries.append(i)

        boundaries.append(len(lines))

        deduped: list[int] = []
        for boundary in sorted(set(boundaries)):
            if not deduped or boundary != deduped[-1]:
                deduped.append(boundary)

        return deduped if len(deduped) > 2 else []

    def _chunk_by_boundaries(self, lines: list[str], boundaries: list[int]) -> list[Chunk]:
        chunks: list[Chunk] = []
        chunk_lines: list[str] = []
        chunk_start = 0
        chunk_index = 0

        for i in range(1, len(boundaries)):
            start = boundaries[i - 1]
            end = boundaries[i]
            section_lines = lines[start:end]

            if len(chunk_lines) + len(section_lines) <= self.target_chunk_size:
                if not chunk_lines:
                    chunk_start = start
                chunk_lines.extend(section_lines)
                continue

            if chunk_lines:
                chunks.append(
                    Chunk(
                        index=chunk_index,
                        start_line=chunk_start + 1,
                        end_line=chunk_start + len(chunk_lines),
                        lines=chunk_lines,
                        reason="boundary",
                    )
                )
                chunk_index += 1

            if len(section_lines) > self.max_chunk_size:
                for fixed in self._chunk_fixed(section_lines, base_index=chunk_index, base_start=start):
                    chunks.append(fixed)
                    chunk_index = fixed.index + 1
                chunk_lines = []
                continue

            chunk_lines = list(section_lines)
            chunk_start = start

        if chunk_lines:
            chunks.append(
                Chunk(
                    index=chunk_index,
                    start_line=chunk_start + 1,
                    end_line=chunk_start + len(chunk_lines),
                    lines=chunk_lines,
                    reason="boundary",
                )
            )

        return chunks

    def _chunk_fixed(self, lines: list[str], base_index: int = 0, base_start: int = 0) -> list[Chunk]:
        chunks: list[Chunk] = []
        chunk_index = base_index

        for start in range(0, len(lines), self.target_chunk_size):
            end = min(start + self.target_chunk_size, len(lines))
            chunk_lines = lines[start:end]
            chunks.append(
                Chunk(
                    index=chunk_index,
                    start_line=base_start + start + 1,
                    end_line=base_start + end,
                    lines=chunk_lines,
                    reason="fixed",
                )
            )
            chunk_index += 1

        return chunks
