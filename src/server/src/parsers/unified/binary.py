from __future__ import annotations

import base64
import logging
import math
import re
import zlib
from dataclasses import dataclass, field

import chardet

logger = logging.getLogger(__name__)

BINARY_EXTENSIONS = frozenset(
    {
        ".bin",
        ".dat",
        ".blob",
        ".logbin",
        ".evtx",
        ".pcap",
        ".pcapng",
        ".db",
        ".sqlite",
        ".parquet",
        ".avro",
        ".orc",
        ".pb",
        ".msgpack",
        ".bson",
    }
)

MAGIC_BYTES: dict[bytes, str] = {
    b"\x50\x4b\x03\x04": "zip",
    b"\x1f\x8b\x08": "gzip",
    b"\x78\x9c": "zlib",
    b"\x78\x01": "zlib",
    b"\x78\xda": "zlib",
    b"\x42\x5a\x68": "bzip2",
    b"\xfd7zXZ\x00": "xz",
    b"\x89\x50\x4e\x47\x0d\x0a\x1a\x0a": "png",
    b"\xff\xd8\xff": "jpeg",
    b"\x25\x50\x44\x46": "pdf",
    b"\x45\x6c\x66\x00": "elf",
    b"\x4d\x5a": "pe_executable",
}

HEX_DUMP_RE = re.compile(r"^[0-9A-Fa-f]{4,8}\s+(?:[0-9A-Fa-f]{2}\s){4,}")
CONTINUOUS_HEX_RE = re.compile(r"^[0-9A-Fa-f]{16,}$")
HEX_DUMP_ASCII_RE = re.compile(r"\|([^|]+)\|\s*$")
CLEARTEXT_SIGNAL_RE = re.compile(rb"[\x20-\x7e]{4,}")
BASE64_START_RE = re.compile(r"^(?:BEGIN_B64|BASE64_START|B64_BEGIN)$", re.IGNORECASE)
BASE64_END_RE = re.compile(r"^(?:END_B64|BASE64_END|B64_END)$", re.IGNORECASE)


@dataclass
class BinaryDecodeResult:
    is_binary: bool = False
    decoded_lines: list[str] = field(default_factory=list)
    format_detected: str = "unknown"
    encoding: str = "unknown"
    entropy: float = 0.0
    warnings: list[str] = field(default_factory=list)


class BinaryHandler:
    def analyze_and_decode(self, content: str | bytes) -> BinaryDecodeResult:
        if isinstance(content, str):
            return self._analyze_text_content(content)
        return self._analyze_binary_content(content)

    def _analyze_text_content(self, content: str) -> BinaryDecodeResult:
        lines = content.splitlines()
        non_printable = sum(1 for line in lines[:100] for c in line if not c.isprintable() and c not in "\n\r\t")
        total_chars = sum(len(line) for line in lines[:100])
        non_printable_ratio = non_printable / max(total_chars, 1)

        if non_printable_ratio < 0.1:
            return BinaryDecodeResult(
                is_binary=False,
                decoded_lines=lines,
                entropy=self._calculate_entropy(content),
            )

        result = BinaryDecodeResult(is_binary=True, entropy=self._calculate_entropy(content))
        result.decoded_lines.extend(self._decode_zlib(content.encode("utf-8", errors="ignore")))
        result.decoded_lines.extend(self._decode_base64_frames(content))
        result.decoded_lines.extend(self._decode_hex_telemetry(content))

        if result.decoded_lines:
            result.format_detected = "encoded_text"
            result.warnings.append(
                f"Binary content detected (non-printable ratio: {non_printable_ratio:.1%}). "
                f"Decoded {len(result.decoded_lines)} lines."
            )
        else:
            result.decoded_lines = self._extract_cleartext_signals(content.encode("utf-8", errors="ignore"))
            if result.decoded_lines:
                result.format_detected = "cleartext_extracted"
                result.warnings.append(f"Extracted {len(result.decoded_lines)} cleartext signals from binary content.")
            else:
                result.warnings.append("Binary content could not be decoded.")

        return result

    def _analyze_binary_content(self, raw_bytes: bytes) -> BinaryDecodeResult:
        result = BinaryDecodeResult(is_binary=True)

        format_detected = self._detect_magic_bytes(raw_bytes)
        if format_detected:
            result.format_detected = format_detected
            result.warnings.append(f"Binary format detected: {format_detected}")

        result.entropy = self._calculate_entropy_bytes(raw_bytes)

        if result.entropy < 4.0:
            decoded = self._decode_bytes_to_text(raw_bytes)
            result.decoded_lines = decoded.splitlines()
            result.encoding = chardet.detect(raw_bytes).get("encoding", "utf-8")
            return result

        result.decoded_lines.extend(self._decode_zlib(raw_bytes))
        result.decoded_lines.extend(self._decode_base64_frames(raw_bytes.decode("utf-8", errors="ignore")))
        result.decoded_lines.extend(self._decode_hex_telemetry(raw_bytes.decode("utf-8", errors="ignore")))
        result.decoded_lines.extend(self._extract_cleartext_signals(raw_bytes))

        if not result.decoded_lines:
            result.warnings.append("Binary content could not be decoded.")

        return result

    @staticmethod
    def _detect_magic_bytes(raw_bytes: bytes) -> str | None:
        for magic, format_name in MAGIC_BYTES.items():
            if raw_bytes.startswith(magic):
                return format_name
        return None

    @staticmethod
    def _decode_zlib(raw_bytes: bytes) -> list[str]:
        lines: list[str] = []
        for magic in (b"\x78\x9c", b"\x78\x01", b"\x78\xda"):
            index = raw_bytes.find(magic)
            if index < 0:
                continue
            try:
                decompressed = zlib.decompress(raw_bytes[index:])
                decoded = BinaryHandler._decode_bytes_to_text(decompressed)
                lines.extend(decoded.splitlines())
                break
            except Exception:
                continue
        return lines

    @staticmethod
    def _decode_base64_frames(text: str) -> list[str]:
        lines = text.splitlines()
        in_frame = False
        chunks: list[str] = []
        decoded_lines: list[str] = []

        for line in lines:
            upper = line.strip().upper()
            if BASE64_START_RE.match(upper):
                in_frame = True
                chunks = []
                continue
            if BASE64_END_RE.match(upper):
                in_frame = False
                payload = "".join(chunks).strip()
                if payload:
                    try:
                        decoded = base64.b64decode(payload, validate=False)
                        decoded_lines.extend(BinaryHandler._decode_bytes_to_text(decoded).splitlines())
                    except Exception:
                        pass
                continue
            if in_frame:
                chunks.append(line.strip())

        return decoded_lines

    @staticmethod
    def _decode_hex_telemetry(text: str) -> list[str]:
        lines: list[str] = []
        for raw_line in text.splitlines():
            stripped = raw_line.strip()
            if not CONTINUOUS_HEX_RE.match(stripped):
                continue
            if len(stripped) % 2 != 0:
                stripped = stripped[:-1]
            try:
                decoded = bytes.fromhex(stripped)
                decoded_line = BinaryHandler._decode_bytes_to_text(decoded).strip()
                if decoded_line:
                    lines.append(decoded_line)
            except Exception:
                continue
        return lines

    @staticmethod
    def _extract_cleartext_signals(raw_bytes: bytes) -> list[str]:
        lines: list[str] = []
        for match in CLEARTEXT_SIGNAL_RE.finditer(raw_bytes):
            candidate = match.group(0)
            try:
                text = candidate.decode("utf-8", errors="ignore").strip()
            except Exception:
                continue
            if text and len(text) >= 4:
                lines.append(text)
        return lines

    @staticmethod
    def _decode_bytes_to_text(raw_bytes: bytes) -> str:
        detected = chardet.detect(raw_bytes)
        encoding = detected.get("encoding") or "utf-8"
        try:
            return raw_bytes.decode(encoding, errors="ignore")
        except Exception:
            return raw_bytes.decode("utf-8", errors="ignore")

    @staticmethod
    def _calculate_entropy(text: str) -> float:
        if not text:
            return 0.0
        freq: dict[str, int] = {}
        for c in text:
            freq[c] = freq.get(c, 0) + 1
        length = len(text)
        entropy = 0.0
        for count in freq.values():
            p = count / length
            if p > 0:
                entropy -= p * math.log2(p)
        return entropy

    @staticmethod
    def _calculate_entropy_bytes(raw_bytes: bytes) -> float:
        if not raw_bytes:
            return 0.0
        freq: dict[int, int] = {}
        for b in raw_bytes:
            freq[b] = freq.get(b, 0) + 1
        length = len(raw_bytes)
        entropy = 0.0
        for count in freq.values():
            p = count / length
            if p > 0:
                entropy -= p * math.log2(p)
        return entropy

    @staticmethod
    def is_binary_extension(filename: str) -> bool:
        return any(filename.lower().endswith(ext) for ext in BINARY_EXTENSIONS)

    @staticmethod
    def extract_ascii_from_hexdump(lines: list[str]) -> list[str]:
        ascii_parts: list[str] = []
        for line in lines:
            match = HEX_DUMP_ASCII_RE.search(line)
            if not match:
                continue
            ascii_text = match.group(1).strip()
            if ascii_text:
                ascii_parts.append(ascii_text)
        return ascii_parts
