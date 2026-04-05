#!/usr/bin/env python3
"""Generate synthetic binary/text hybrid semiconductor logs.

Produces files with:
  - Plain text log lines (timestamps, levels, messages)
  - Hex dump sections (equipment memory dumps)
  - Base64-encoded telemetry frames
  - Zlib-compressed data blocks
  - Embedded ERRCODE= and WARN: signals in binary noise
  - Mixed encodings (UTF-8 + raw bytes)

Usage:
    python scripts/generate_hybrid_logs.py -o hybrid_test.log -n 1000
    python scripts/generate_hybrid_logs.py --binary-ratio 0.3 -o mixed.log
"""

import argparse
import base64
import random
import struct
import zlib
from datetime import datetime, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TOOLS = ["TOOL-CVD01", "TOOL-ETCH02", "TOOL-PVD03", "TOOL-CMP04", "EQP-LITHO05"]
WAFERS = [f"W{i:04d}" for i in range(1, 50)]
LOTS = ["LOT-A2024", "LOT-B3091", "FOUP-C117", "LOT-D4455"]
RECIPES = ["RCP-OXIDE-DEP", "RCP-SI-ETCH", "RCP-TIN-PVD", "RCP-CU-CMP", "RCP-PHOTO-EXP"]
SUBSYSTEMS = ["RF_generator", "vacuum_system", "gas_delivery", "wafer_handler", "thermal_control"]
CHAMBERS = ["CHAMBER-A", "CHAMBER-B", "CHAMBER-C", "CHAMBER-D"]
RF_CODES = [f"RF_{i}" for i in range(100, 300)]

LOG_LEVELS = ["INFO", "WARN", "ERROR", "DEBUG", "CRITICAL"]
LEVEL_WEIGHTS = [50, 20, 10, 15, 5]

# ---------------------------------------------------------------------------
# Text log templates
# ---------------------------------------------------------------------------

TEXT_TEMPLATES = [
    "{ts} INFO  [{tool}] Process started: recipe={recipe} wafer={wafer} chamber={chamber}",
    "{ts} INFO  [{tool}] Process completed: recipe={recipe} wafer={wafer} duration={duration}s",
    "{ts} INFO  [{tool}] Wafer loaded: {wafer} slot={slot} lot={lot}",
    "{ts} INFO  [{tool}] Wafer unloaded: {wafer} slot={slot}",
    "{ts} WARN  [{tool}] RF reflected power high: reflected={reflected}W threshold=50W rf_code={rf_code}",
    "{ts} ERROR [{tool}] ERRCODE=0x{errcode:04X} Chamber pressure out of range: pressure={pressure}mTorr",
    "{ts} ERROR [{tool}] Interlock triggered: subsystem={subsystem} ERRCODE=0x{errcode:04X}",
    "{ts} INFO  [{tool}] Measurement: thickness={thickness}nm uniformity={uniformity}% wafer={wafer}",
    "{ts} INFO  [{tool}] Gas flow: SiH4={sih4}sccm N2={n2}sccm O2={o2}sccm",
    "{ts} DEBUG [{tool}] Heartbeat: status=OK uptime={uptime}h temperature={temp}C",
    "{ts} DEBUG [{tool}] Heartbeat: status=OK uptime={uptime}h temperature={temp}C",
    "{ts} DEBUG [{tool}] Heartbeat: status=OK uptime={uptime}h temperature={temp}C",
    "{ts} INFO  [{tool}] Recipe change: from={recipe} to={recipe2} operator=jchen",
    "{ts} WARN  [{tool}] Vibration detected: vibration_rms={vibration}mm/s subsystem={subsystem}",
    "{ts} INFO  [{tool}] Calibration: target={target}nm actual={actual}nm delta={delta}nm",
    "{ts} CRITICAL [{tool}] ERRCODE=0x{errcode:04X} Emergency shutdown: {subsystem} failure",
    "{ts} INFO  [{tool}] Particle count: count={particles} threshold=100 wafer={wafer}",
    "{ts} INFO  [{tool}] Vacuum level: vacuum={vacuum}mTorr pump_speed={rpm}rpm",
    "{ts} WARN  [{tool}] Temperature drift: temp={temp}C target={target_temp}C rate={rate}C/min",
    "{ts} INFO  [{tool}] RF power: forward={forward}W reflected={reflected}W frequency={freq}MHz",
    "{ts} INFO  [{tool}] Step transition: STEP={step} -> STEP={step2} wafer={wafer}",
    "{ts} INFO  [{tool}] Resistivity: resistivity={resistivity}MOhm wafer={wafer}",
]

# Heartbeat templates (high frequency, no actionable content)
HEARTBEAT_TEMPLATES = [
    "{ts} DEBUG [{tool}] Heartbeat: status=OK uptime={uptime}h",
    "{ts} DEBUG [{tool}] System alive: pid=1234 mem=45%",
    "{ts} INFO  [{tool}] Periodic check: all_ok=true",
]


def _random_ts(base: datetime, offset_seconds: int) -> str:
    """Generate a random ISO-8601 timestamp."""
    dt = base + timedelta(seconds=offset_seconds)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _random_text_line(ts: str) -> str:
    """Generate a random text log line."""
    template = random.choice(TEXT_TEMPLATES)
    return template.format(
        ts=ts,
        tool=random.choice(TOOLS),
        wafer=random.choice(WAFERS),
        lot=random.choice(LOTS),
        recipe=random.choice(RECIPES),
        recipe2=random.choice(RECIPES),
        chamber=random.choice(CHAMBERS),
        subsystem=random.choice(SUBSYSTEMS),
        rf_code=random.choice(RF_CODES),
        errcode=random.randint(0x0001, 0xFFFF),
        duration=round(random.uniform(30, 600), 1),
        slot=random.randint(1, 25),
        reflected=round(random.uniform(10, 80), 1),
        pressure=round(random.uniform(0.1, 100), 2),
        thickness=round(random.uniform(50, 500), 1),
        uniformity=round(random.uniform(90, 99.9), 1),
        sih4=round(random.uniform(10, 200), 0),
        n2=round(random.uniform(100, 2000), 0),
        o2=round(random.uniform(5, 100), 0),
        uptime=random.randint(1, 10000),
        temp=round(random.uniform(20, 400), 1),
        vibration=round(random.uniform(0.01, 5.0), 3),
        target=round(random.uniform(100, 300), 1),
        actual=round(random.uniform(100, 300), 1),
        delta=round(random.uniform(-5, 5), 2),
        particles=random.randint(0, 500),
        vacuum=round(random.uniform(0.001, 10), 3),
        rpm=random.randint(1000, 5000),
        target_temp=round(random.uniform(100, 400), 1),
        rate=round(random.uniform(0.1, 10), 2),
        forward=round(random.uniform(100, 2000), 0),
        freq=round(random.uniform(13.56, 60), 2),
        step=random.randint(1, 20),
        step2=random.randint(1, 20),
        resistivity=round(random.uniform(1, 100), 2),
    )


def _random_heartbeat_line(ts: str) -> str:
    """Generate a heartbeat line."""
    template = random.choice(HEARTBEAT_TEMPLATES)
    return template.format(
        ts=ts,
        tool=random.choice(TOOLS),
        uptime=random.randint(1, 10000),
    )


# ---------------------------------------------------------------------------
# Binary section generators
# ---------------------------------------------------------------------------


def _generate_hex_dump(num_lines: int = 8) -> list[str]:
    """Generate a hex dump section with ASCII sidebar."""
    lines = [f"--- HEX_DUMP: {random.choice(SUBSYSTEMS)} memory snapshot ---"]
    for i in range(num_lines):
        offset = i * 16
        hex_bytes = " ".join(f"{random.randint(0, 255):02X}" for _ in range(16))
        ascii_chars = "".join(
            chr(random.randint(0x20, 0x7E)) if random.random() > 0.3 else "."
            for _ in range(16)
        )
        lines.append(f"{offset:08X}  {hex_bytes}  |{ascii_chars}|")
    return lines


def _generate_base64_frame() -> list[str]:
    """Generate a base64-encoded telemetry frame."""
    # Create a fake telemetry payload
    payload = {
        "tool": random.choice(TOOLS),
        "wafer": random.choice(WAFERS),
        "pressure": round(random.uniform(0.1, 100), 3),
        "temperature": round(random.uniform(20, 400), 1),
        "rf_power": round(random.uniform(100, 2000), 0),
    }
    raw = str(payload).encode("utf-8")
    encoded = base64.b64encode(raw).decode("ascii")

    lines = ["BEGIN_B64"]
    # Split into 76-char lines
    for i in range(0, len(encoded), 76):
        lines.append(encoded[i : i + 76])
    lines.append("END_B64")
    return lines


def _generate_zlib_block() -> list[str]:
    """Generate a zlib-compressed data block marker."""
    payload = f"ERRCODE=0x{random.randint(0x100, 0xFFFF):04X} WARN: {random.choice(SUBSYSTEMS)} anomaly detected"
    compressed = zlib.compress(payload.encode("utf-8"))
    hex_repr = compressed.hex().upper()

    lines = [
        f"--- ZLIB_BLOCK: compressed telemetry ({len(compressed)} bytes) ---",
        hex_repr,
        "--- END_ZLIB_BLOCK ---",
    ]
    return lines


def _generate_binary_noise_with_signals() -> list[str]:
    """Generate binary noise with embedded cleartext signals."""
    lines = []
    # Generate some binary-looking hex strings with embedded signals
    signals = [
        f"ERRCODE=0x{random.randint(0x100, 0xFFFF):04X}",
        f"WARN: {random.choice(SUBSYSTEMS)} threshold exceeded",
        f"EVT{random.randint(100, 999)} code={random.choice(RF_CODES)}",
    ]

    noise = random.randbytes(64).hex().upper()
    signal = random.choice(signals)
    lines.append(f"--- BINARY_SIGNAL: {noise[:32]}...{signal}...{noise[32:64]} ---")
    return lines


def _generate_continuous_hex() -> list[str]:
    """Generate continuous hex telemetry (no offset, no ASCII sidebar)."""
    hex_data = random.randbytes(random.randint(32, 128)).hex().upper()
    return [f"--- CONTINUOUS_HEX ---", hex_data, "--- END_HEX ---"]


# ---------------------------------------------------------------------------
# Main generator
# ---------------------------------------------------------------------------


def generate_hybrid_log(
    num_lines: int = 1000,
    binary_ratio: float = 0.25,
    heartbeat_ratio: float = 0.15,
    seed: int | None = None,
) -> str:
    """Generate a hybrid binary/text semiconductor log.

    Parameters
    ----------
    num_lines:
        Approximate total number of output lines.
    binary_ratio:
        Fraction of lines that are binary sections (hex dumps, base64, etc.).
    heartbeat_ratio:
        Fraction of text lines that are heartbeats.
    seed:
        Random seed for reproducibility.
    """
    if seed is not None:
        random.seed(seed)

    base_time = datetime(2026, 3, 15, 8, 0, 0)
    output_lines: list[str] = []
    line_count = 0
    second_offset = 0

    binary_generators = [
        (_generate_hex_dump, 0.35),
        (_generate_base64_frame, 0.25),
        (_generate_zlib_block, 0.15),
        (_generate_binary_noise_with_signals, 0.15),
        (_generate_continuous_hex, 0.10),
    ]

    while line_count < num_lines:
        second_offset += random.randint(1, 30)
        ts = _random_ts(base_time, second_offset)

        if random.random() < binary_ratio:
            # Generate a binary section
            gen_func = random.choices(
                [g[0] for g in binary_generators],
                weights=[g[1] for g in binary_generators],
            )[0]
            section = gen_func()
            output_lines.extend(section)
            line_count += len(section)
        elif random.random() < heartbeat_ratio:
            # Generate a heartbeat line
            output_lines.append(_random_heartbeat_line(ts))
            line_count += 1
        else:
            # Generate a text log line
            output_lines.append(_random_text_line(ts))
            line_count += 1

    return "\n".join(output_lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate synthetic binary/text hybrid semiconductor logs."
    )
    parser.add_argument(
        "-o", "--output", type=Path, default=Path("hybrid_test.log"),
        help="Output file path (default: hybrid_test.log)",
    )
    parser.add_argument(
        "-n", "--num-lines", type=int, default=1000,
        help="Approximate number of output lines (default: 1000)",
    )
    parser.add_argument(
        "--binary-ratio", type=float, default=0.25,
        help="Fraction of lines that are binary sections (default: 0.25)",
    )
    parser.add_argument(
        "--heartbeat-ratio", type=float, default=0.15,
        help="Fraction of text lines that are heartbeats (default: 0.15)",
    )
    parser.add_argument(
        "--seed", type=int, default=None,
        help="Random seed for reproducibility",
    )
    args = parser.parse_args()

    content = generate_hybrid_log(
        num_lines=args.num_lines,
        binary_ratio=args.binary_ratio,
        heartbeat_ratio=args.heartbeat_ratio,
        seed=args.seed,
    )

    args.output.write_text(content, encoding="utf-8")
    actual_lines = content.count("\n") + 1
    print(f"Generated {actual_lines} lines -> {args.output} ({len(content)} bytes)")


if __name__ == "__main__":
    main()
