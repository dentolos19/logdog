"""Binary file detection, safe decoding, and string extraction utilities.

Provides heuristics for distinguishing binary data from text content,
safe fallback decoding for mixed text, and printable-string extraction
(similar to the UNIX ``strings`` utility) for embedded human-readable text.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any

# Thresholds for binary detection
NUL_BYTE_RATIO_THRESHOLD = 0.05  # >5% NUL bytes → likely binary
HIGH_BIT_RATIO_THRESHOLD = 0.30  # >30% bytes with high-bit set → likely binary
CONTROL_CHAR_RATIO_THRESHOLD = 0.20  # >20% control chars (excluding common whitespace) → likely binary
MIN_STRING_LENGTH = 4  # minimum length for embedded string extraction
HEX_PREVIEW_LENGTH = 256  # max bytes for hex preview

# Known magic bytes for common binary formats
KNOWN_MAGIC: dict[bytes, str] = {
    b"\x89PNG\r\n\x1a\n": "PNG image",
    b"\xff\xd8\xff": "JPEG image",
    b"GIF8": "GIF image",
    b"%PDF": "PDF document",
    b"PK\x03\x04": "ZIP archive",
    b"\x1f\x8b\x08": "GZIP archive",
    b"\x42\x5a\x68": "BZ2 archive",
    b"\xfd7zXZ\x00": "XZ archive",
    b"\xcf\xfa\xed\xfe": "Mach-O 32-bit",
    b"\xce\xfa\xed\xfe": "Mach-O 64-bit",
    b"\x7fELF": "ELF binary",
    b"MZ": "PE/DOS executable",
    b"PLC\x01": "PLC snapshot (logdog)",
    b"PLC\x02": "PLC snapshot v2 (logdog)",
    b"\xef\xbb\xbf": "UTF-8 BOM text",
    b"\xff\xfe": "UTF-16 LE text",
    b"\xfe\xff": "UTF-16 BE text",
}
MAX_MAGIC_LEN = max(len(m) for m in KNOWN_MAGIC)


EXTENSION_BINARY_SET: frozenset[str] = frozenset(
    {
        ".bin",
        ".exe",
        ".dll",
        ".so",
        ".dylib",
        ".o",
        ".obj",
        ".lib",
        ".a",
        ".class",
        ".pyc",
        ".pyo",
        ".pyd",
        ".whl",
        ".egg",
        ".jar",
        ".war",
        ".dex",
        ".apk",
        ".ipa",
        ".iso",
        ".img",
        ".vhd",
        ".vmdk",
        ".qcow2",
        ".pcap",
        ".cap",
        ".dump",
        ".core",
        ".dmp",
        ".db",
        ".sqlite",
        ".s3db",
        ".ttf",
        ".otf",
        ".woff",
        ".woff2",
        ".eot",
        ".ico",
        ".icns",
        ".wav",
        ".mp3",
        ".aac",
        ".flac",
        ".ogg",
        ".wma",
        ".mp4",
        ".avi",
        ".mkv",
        ".mov",
        ".wmv",
        ".webm",
        ".zipx",
        ".rar",
        ".7z",
        ".lz",
        ".lzma",
    }
)

TEXT_LIKE_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".txt",
        ".log",
        ".csv",
        ".tsv",
        ".json",
        ".xml",
        ".yaml",
        ".yml",
        ".toml",
        ".cfg",
        ".conf",
        ".ini",
        ".md",
        ".rst",
        ".html",
        ".htm",
        ".xhtml",
        ".svg",
        ".css",
        ".js",
        ".jsx",
        ".ts",
        ".tsx",
        ".py",
        ".rb",
        ".sh",
        ".bash",
        ".zsh",
        ".env",
        ".gitignore",
        ".dockerignore",
        ".editorconfig",
    }
)


def is_probably_binary(
    raw_bytes: bytes,
    filename: str = "",
    mime_type: str | None = None,
) -> bool:
    """Determine whether *raw_bytes* is likely binary (not human-readable text).

    Uses a combination of file extension, MIME type, NUL byte ratio,
    high-bit ratio, control-character ratio, and magic-byte detection.
    """
    # Fast path: known binary extension
    if filename:
        ext = _get_extension(filename)
        if ext in TEXT_LIKE_EXTENSIONS:
            return False
        if ext in EXTENSION_BINARY_SET:
            return True

    # Fast path: known binary MIME type
    if mime_type:
        if mime_type.startswith("text/"):
            return False
        if mime_type in (
            "application/octet-stream",
            "application/x-binary",
            "application/x-msdownload",
        ):
            return True

    if not raw_bytes:
        return False

    length = len(raw_bytes)
    if length == 0:
        return False

    # Count byte categories
    nul_count = 0
    high_bit_count = 0
    control_count = 0

    for byte in raw_bytes:
        if byte == 0:
            nul_count += 1
        elif byte >= 128:
            high_bit_count += 1
        elif byte < 32 and byte not in (9, 10, 13):  # not tab/LF/CR
            control_count += 1

    # NUL byte is a strong binary signal
    if length > 10 and nul_count / length > NUL_BYTE_RATIO_THRESHOLD:
        return True

    # High ratio of control chars (excluding common whitespace)
    if length > 10 and control_count / length > CONTROL_CHAR_RATIO_THRESHOLD:
        return True

    # High-bit ratio
    if length > 10 and high_bit_count / length > HIGH_BIT_RATIO_THRESHOLD:
        return True

    return False


def _get_extension(filename: str) -> str:
    """Return the lowercase file extension, including the dot."""
    idx = filename.rfind(".")
    if idx == -1:
        return ""
    return filename[idx:].lower()


def detect_magic(raw_bytes: bytes) -> tuple[str, str]:
    """Detect magic bytes in *raw_bytes*.

    Returns ``(magic_hex, label)``. If no known magic is found,
    the first 4 raw bytes are returned as *magic_hex* and
    ``"unknown"`` as the label.
    """
    if not raw_bytes:
        return ("", "empty")

    # Try known magic patterns (longest first)
    for magic, label in sorted(KNOWN_MAGIC.items(), key=lambda x: -len(x[0])):
        if raw_bytes.startswith(magic):
            return (magic.hex(" "), label)

    # Return first 4 bytes as raw hex
    preview = raw_bytes[:4].hex(" ")
    return (preview, "unknown")


def safe_decode_text(raw_bytes: bytes, errors: str = "replace") -> str:
    """Decode *raw_bytes* to text safely.

    Tries UTF-8 first, then Latin-1, then a best-effort with *errors*.
    Returns the decoded string.
    """
    try:
        return raw_bytes.decode("utf-8")
    except UnicodeDecodeError:
        try:
            return raw_bytes.decode("latin-1")
        except UnicodeDecodeError:
            return raw_bytes.decode("utf-8", errors=errors)


def extract_printable_strings(
    raw_bytes: bytes,
    min_length: int = MIN_STRING_LENGTH,
) -> list[str]:
    """Extract contiguous printable ASCII strings from *raw_bytes*.

    Similar to the UNIX ``strings`` utility. Returns list of strings
    of length >= *min_length*.
    """
    strings: list[str] = []
    current: list[str] = []

    for byte in raw_bytes:
        # printable ASCII or tab
        if 32 <= byte <= 126 or byte == 9:
            current.append(chr(byte))
        else:
            if len(current) >= min_length:
                strings.append("".join(current))
            current = []

    if len(current) >= min_length:
        strings.append("".join(current))

    return strings


def extract_printable_text(
    raw_bytes: bytes,
    min_length: int = MIN_STRING_LENGTH,
    separator: str = "\n",
) -> str:
    """Extract printable strings and join them into a text block."""
    return separator.join(extract_printable_strings(raw_bytes, min_length))


def preview_hex(raw_bytes: bytes, max_bytes: int = HEX_PREVIEW_LENGTH) -> str:
    """Return a space-separated hex preview of the first *max_bytes*."""
    if not raw_bytes:
        return ""
    preview = raw_bytes[:max_bytes]
    return preview.hex(" ")


def sha256_bytes(raw_bytes: bytes) -> str:
    """Return the SHA-256 hex digest of *raw_bytes*."""
    return hashlib.sha256(raw_bytes).hexdigest()


def binary_metadata(
    raw_bytes: bytes,
    filename: str = "",
    mime_type: str | None = None,
) -> dict[str, Any]:
    """Return a metadata dict for binary data.

    Includes byte length, SHA-256, magic bytes, and whether the
    data is classified as binary.
    """
    if not raw_bytes:
        return {
            "byte_length": 0,
            "is_binary": False,
        }

    magic_hex, magic_label = detect_magic(raw_bytes)
    is_binary = is_probably_binary(raw_bytes, filename, mime_type)

    return {
        "byte_length": len(raw_bytes),
        "sha256": sha256_bytes(raw_bytes),
        "magic_hex": magic_hex,
        "magic_label": magic_label,
        "is_binary": is_binary,
        "has_nul": b"\x00" in raw_bytes,
    }
