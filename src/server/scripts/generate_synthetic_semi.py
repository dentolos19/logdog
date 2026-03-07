#!/usr/bin/env python3
"""Generate synthetic semiconductor unstructured log data.

Usage:
    python scripts/generate_synthetic_semi.py [-o output.log] [-n 50]

Produces realistic-looking plain-text log lines resembling semiconductor
fab tool logs, equipment alarms, process step transitions, and metrology
readouts — the exact kind of unstructured data our parser handles.
"""

import argparse
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

TOOLS = ["EQP-CVD-01", "EQP-ETCH-03", "EQP-LITHO-02", "TOOL-CMP-04", "CHAMBER-PVD-01"]
WAFERS = ["W0012", "W0045", "W0078", "W0113", "W0200"]
LOTS = ["LOT-A2024", "LOT-B3019", "LOT-C4501", "FOUP-X012"]
RECIPES = ["RCP-OX-THIN", "RECIPE-NITRIDE-200", "RCP-POLY-ETCH", "RCP-METAL-DEP"]
STEPS = ["PRE_CLEAN", "DEPOSITION", "ETCH", "INSPECTION", "POST_CLEAN", "ALIGNMENT", "EXPOSURE"]
LEVELS = ["INFO", "WARN", "ERROR", "DEBUG"]
GASES = ["N2", "O2", "Ar", "CF4", "C4F8", "SiH4", "NH3"]

TEMPLATES = [
    "{ts} {level} [{tool}] Wafer {wafer} loaded into chamber. Recipe={recipe} Step={step}",
    "{ts} {level} [{tool}] Process started for {lot}, wafer {wafer}. Target temp={temp}°C pressure={pressure}mTorr",
    "{ts} {level} [{tool}] Gas flow established: {gas}={flow}sccm. Stability check PASSED.",
    "{ts} {level} [{tool}] ALARM: Temperature deviation on {wafer}. Actual={actual_temp}°C Expected={temp}°C delta={delta}°C",
    "{ts} {level} [{tool}] Step {step} completed. Duration={duration}s. Endpoint detected at t={endpoint}s",
    "{ts} {level} [{tool}] Metrology readout for {wafer}: thickness={thickness}nm uniformity={uniformity}%",
    "{ts} {level} [{tool}] Wafer {wafer} unloaded. Total process time={total_time}s. Result=OK",
    "{ts} {level} [{tool}] Wafer {wafer} unloaded. Total process time={total_time}s. Result=FAIL defect_count={defects}",
    "{ts} {level} [{tool}] Preventive maintenance reminder: {pm_hours}h since last PM. Threshold=500h",
    "{ts} {level} [{tool}] Chamber idle. Standby gas flow: {gas}={flow}sccm. Vacuum={vacuum}mTorr",
    "{ts} {level} [{tool}] RF power set to {rf_power}W for {step} on {wafer}. Reflected={reflected}W",
    "{ts} {level} [{tool}] ERROR: Interlock triggered — {interlock_reason}. Wafer {wafer} aborted.",
    "{ts} {level} [{tool}] Lot {lot} started. Wafers in cassette: {wafer_count}. Priority={priority}",
    "{ts} {level} [{tool}] Particle count on {wafer}: {particle_count} (limit={particle_limit}). {particle_result}",
    "--- SYSTEM: scheduler heartbeat at {ts}. Active tools: {active_tools}. Queue depth: {queue_depth} ---",
    "{ts} {level} [{tool}] Software version check: v{sw_ver}. Config hash={config_hash}",
]

INTERLOCK_REASONS = [
    "pressure spike detected",
    "gas leak sensor triggered",
    "door open during process",
    "RF reflected power exceeded limit",
    "temperature runaway detected",
    "vacuum loss in loadlock",
]


def _random_ts(base: datetime, offset_seconds: int) -> str:
    ts = base + timedelta(seconds=offset_seconds)
    return ts.strftime("%Y-%m-%d %H:%M:%S.") + f"{random.randint(0, 999):03d}"


def generate_line(base_time: datetime, offset: int) -> str:
    template = random.choice(TEMPLATES)
    ts = _random_ts(base_time, offset)
    tool = random.choice(TOOLS)
    wafer = random.choice(WAFERS)
    lot = random.choice(LOTS)
    recipe = random.choice(RECIPES)
    step = random.choice(STEPS)
    level = random.choice(LEVELS)

    return template.format(
        ts=ts,
        level=level,
        tool=tool,
        wafer=wafer,
        lot=lot,
        recipe=recipe,
        step=step,
        temp=random.randint(200, 900),
        pressure=random.randint(1, 500),
        gas=random.choice(GASES),
        flow=random.randint(10, 500),
        actual_temp=random.randint(200, 950),
        delta=round(random.uniform(0.5, 15.0), 1),
        duration=random.randint(30, 600),
        endpoint=random.randint(20, 500),
        thickness=round(random.uniform(5.0, 500.0), 1),
        uniformity=round(random.uniform(90.0, 99.9), 1),
        total_time=random.randint(60, 1800),
        defects=random.randint(1, 50),
        pm_hours=random.randint(100, 600),
        vacuum=round(random.uniform(0.01, 5.0), 2),
        rf_power=random.randint(50, 3000),
        reflected=random.randint(0, 50),
        interlock_reason=random.choice(INTERLOCK_REASONS),
        wafer_count=random.randint(1, 25),
        priority=random.choice(["normal", "hot", "urgent"]),
        particle_count=random.randint(0, 200),
        particle_limit=100,
        particle_result=random.choice(["PASS", "FAIL"]),
        active_tools=random.randint(1, 5),
        queue_depth=random.randint(0, 12),
        sw_ver=f"{random.randint(2, 5)}.{random.randint(0, 9)}.{random.randint(0, 99)}",
        config_hash=f"{random.randint(0, 0xFFFFFFFF):08x}",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic semiconductor log data.")
    parser.add_argument("-o", "--output", type=Path, default=Path("scripts/synthetic_semi.log"))
    parser.add_argument("-n", "--lines", type=int, default=50)
    args = parser.parse_args()

    base_time = datetime(2025, 6, 15, 8, 0, 0, tzinfo=timezone.utc)
    lines: list[str] = []

    for i in range(args.lines):
        offset = i * random.randint(1, 30)
        lines.append(generate_line(base_time, offset))

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Generated {len(lines)} lines → {args.output}")


if __name__ == "__main__":
    main()
