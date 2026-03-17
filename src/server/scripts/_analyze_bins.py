"""Analyze binary test files to understand their structure."""

import re
import struct
import zlib
from pathlib import Path

DATA_DIR = Path(r"c:\2025-Semester-2\Logdog Hackathon\unstructured sample data")

# === zlib_embedded_02.bin ===
raw = (DATA_DIR / "zlib_embedded_02.bin").read_bytes()
print(f"=== zlib_embedded_02.bin ({len(raw)} bytes) ===")
idx = raw.find(b"ZLIB")
print(f"  ZLIB marker at offset: {idx}")
zlib_start = raw.find(b"\x78\x9c")
print(f"  zlib header (78 9C) at offset: {zlib_start}")
if zlib_start >= 0:
    try:
        decompressed = zlib.decompress(raw[zlib_start:])
        print(f"  Decompressed: {len(decompressed)} bytes")
        print(f"  Content: {decompressed[:500]}")
    except Exception as e:
        print(f"  Decompress error: {e}")

print()

# === tool_event_blob_01.bin ===
raw = (DATA_DIR / "tool_event_blob_01.bin").read_bytes()
print(f"=== tool_event_blob_01.bin ({len(raw)} bytes) ===")
for m in re.finditer(rb"EVT\d+\s+code=\w+\s+msg=[^\x00]+", raw):
    text = m.group().decode("ascii")
    print(f"  offset {m.start()}: {text}")

print()

# === noisy_mixed_05.bin ===
raw = (DATA_DIR / "noisy_mixed_05.bin").read_bytes()
print(f"=== noisy_mixed_05.bin ({len(raw)} bytes) ===")
for m in re.finditer(rb"[\x20-\x7e]{4,}", raw):
    text = m.group().decode("ascii")
    print(f"  offset {m.start()}: {text}")

print()

# === telemetry_hex_ascii_04.bin ===
raw = (DATA_DIR / "telemetry_hex_ascii_04.bin").read_bytes()
text = raw.decode("ascii", errors="replace")
lines = text.strip().split("\n")
print(f"=== telemetry_hex_ascii_04.bin ({len(raw)} bytes, {len(lines)} lines) ===")
for line in lines:
    print(f"  line: {line[:80]}")

hex_str = lines[1] if len(lines) > 1 else lines[0]
print(f"\n  Hex string length: {len(hex_str)} chars = {len(hex_str) // 2} bytes")
byte_data = bytes.fromhex(hex_str)
print(f"  Decoded bytes: {len(byte_data)} bytes")
# Try parsing as float records
for i in range(0, min(len(byte_data), 112), 14):
    chunk = byte_data[i : i + 14]
    hex_repr = " ".join(f"{b:02X}" for b in chunk)
    # Try interpreting as: 4-byte ID + 2-byte field + 4-byte float(BE) + 4-byte extra
    if len(chunk) >= 10:
        sensor_id = chunk[0:4].hex().upper()
        val_bytes = chunk[4:8]
        float_val = struct.unpack(">f", val_bytes)[0] if len(val_bytes) == 4 else 0
        print(f"  record {i // 14}: {hex_repr}  (sensor={sensor_id}, reading={float_val:.4f})")

print()

# === base64_framed_03.bin ===
raw = (DATA_DIR / "base64_framed_03.bin").read_bytes()
print(f"=== base64_framed_03.bin ({len(raw)} bytes) ===")
# Find BEGIN_B64 and END_B64 markers
begin_idx = raw.find(b"BEGIN_B64")
end_idx = raw.find(b"END_B64")
print(f"  BEGIN_B64 at offset: {begin_idx}")
print(f"  END_B64 at offset: {end_idx}")
if begin_idx >= 0 and end_idx >= 0:
    payload = raw[begin_idx + len(b"BEGIN_B64") : end_idx]
    payload_text = payload.decode("ascii", errors="replace").strip()
    print(f"  Payload ({len(payload_text)} chars): {payload_text[:100]}...")
    # Check what's after END_B64
    after = raw[end_idx + len(b"END_B64") :]
    print(f"  After END_B64 ({len(after)} bytes): {after.hex()}")
    # Before BEGIN_B64
    before = raw[:begin_idx]
    print(f"  Before BEGIN_B64 ({len(before)} bytes): {before.hex()}")
    print(f"  Header: {before}")
