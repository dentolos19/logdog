#!/usr/bin/env python3
"""Generate truly unstructured semiconductor log samples.

Unlike synthetic_semi.log (which has a consistent timestamp+level+[tool] format),
these logs simulate real-world unstructured data:
  - Free-form operator notes without timestamps
  - Equipment alarm dumps with inconsistent formatting
  - Mixed prose and measurements
  - Maintenance records
  - Various or missing timestamp formats
  - Multiline entries (stack traces, alarm details)

Usage:
    python scripts/generate_unstructured_logs.py [-o output.log] [-n 60]
"""

import argparse
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

TOOLS = ["EQP-CVD-01", "EQP-ETCH-03", "TOOL-CMP-04", "CHAMBER-PVD-01"]
WAFERS = ["W0012", "W0045", "W0078", "W0113", "W0200"]
LOTS = ["LOT-A2024", "LOT-B3019", "FOUP-X012"]
RECIPES = ["RCP-OX-THIN", "RECIPE-NITRIDE-200", "RCP-POLY-ETCH"]
STEPS = ["PRE_CLEAN", "DEPOSITION", "ETCH", "INSPECTION", "POST_CLEAN"]
GASES = ["N2", "O2", "Ar", "CF4", "SiH4"]
OPERATORS = ["jchen", "mwilliams", "akumar", "sliu"]

# Truly unstructured templates -- irregular formatting, prose, missing fields.
TEMPLATES = [
    # Operator free-form notes
    "Operator {op} logged chamber condition: slight discoloration on showerhead, scheduled cleaning after lot {lot} completes",
    "NOTE from {op}: wafer {wafer} shows edge exclusion issue, possibly chuck alignment on {tool}. Will monitor next 3 runs.",
    "Shift handover - {op} to night shift. {tool} running recipe {recipe}, no issues. Particle counts nominal.",
    "{op} override: skipped Step {step} for {wafer} per engineering request #E-{eng_num}. Deviation form attached.",

    # Equipment alarms with no consistent format
    "*** ALARM *** {tool} pressure spike at {ts_short} -- chamber pressure jumped to {pressure}mTorr, interlock NOT tripped",
    "FAULT {tool}: RF matchbox tuning failure during {step}. Reflected power={reflected}W (limit 50W). Wafer {wafer} aborted mid-process.",
    "{tool} reported ERRCODE={errcode} at {ts_short}. Subsystem: gas delivery. Operator acknowledged.",
    "WARNING -- {tool} temp sensor reading {actual_temp}C on zone 3, expected {temp}C. Delta={delta}C. No corrective action taken yet.",

    # Maintenance logs (prose)
    "PM completed on {tool} by tech {op}. Replaced showerhead (P/N SH-{pn}), cleaned chamber walls, ran seasoning recipe x3. Particle qual: {particle_count} counts (spec <100).",
    "Scheduled downtime: {tool} offline for {downtime}hrs starting {ts_date}. Reason: quarterly preventive maintenance and RF generator calibration.",
    "Leak check results for {tool} loadlock: He rate = {leak_rate}E-9 atm-cc/s. PASS (spec < 1.0E-8).",
    "{tool} turbo pump vibration trending upward -- {vibration}mm/s RMS. Threshold=2.5mm/s. Ordering replacement (ETA 5 days).",

    # Metrology readouts with prose context
    "Post-dep measurement on {wafer} (lot {lot}): film thickness={thickness}nm across 49 sites, uniformity={uniformity}%, target was {target_thick}nm +/-5%.",
    "Metrology flag: {wafer} thickness out of spec at {thickness}nm (target {target_thick}nm). {tool} recipe {recipe} may need adjustment.",
    "Particle inspection {wafer}: {particle_count} adders >0.1um detected. Previous wafer had {prev_particles}. Possible chamber contamination on {tool}.",

    # Process logs without timestamps
    "Gas stabilization phase: {gas}={flow}sccm, total flow={total_flow}sccm, chamber pressure settling at {pressure}mTorr",
    "Endpoint detection triggered at {endpoint}s into {step} step. Emission intensity drop of {intensity_drop}% on {gas} line.",
    "Chuck temperature ramping: current={actual_temp}C target={temp}C rate=5C/min on {tool}",

    # Mixed format entries
    "Run #{run_num} | {tool} | {wafer} | {recipe} | {step} | result: {result} | thickness={thickness}nm",
    "[{ts_short}] {tool} >> process log: started {recipe} on {wafer}, lot={lot}, step={step}, RF={rf_power}W bias={bias}V",
    "{ts_long} --- Equipment event --- {tool}: door opened by operator {op}. Wafer {wafer} on chuck. Process PAUSED.",

    # Multiline alarm dumps
    "=== CRITICAL ALARM: {tool} ===\n  Timestamp: {ts_long}\n  Wafer: {wafer}\n  Error: vacuum loss in process chamber\n  Pressure: {pressure}mTorr (expected < 5mTorr)\n  Action: process aborted, wafer quarantined",
    "INCIDENT REPORT\n  Tool: {tool}\n  Date: {ts_date}\n  Operator: {op}\n  Description: {gas} mass flow controller stuck at {flow}sccm during {step}\n  Impact: wafer {wafer} scrapped, lot {lot} on hold",

    # Cryptic equipment messages
    "SYS:{tool}:MOD3:GAS_PANEL valve V-{valve_num} timeout after {timeout}ms. State=OPENING. Retry {retry}/3.",
    "DI water resistivity={resistivity}MOhm-cm (spec>17.5). Rinse cycle {wafer} complete.",
    "{tool} self-test sequence: chuck vacuum OK, He backside {he_pressure}Torr OK, lift pins OK, clamp OK. Ready.",
]


def _ts_short(base: datetime, offset: int) -> str:
    ts = base + timedelta(seconds=offset)
    return ts.strftime("%H:%M:%S")


def _ts_long(base: datetime, offset: int) -> str:
    ts = base + timedelta(seconds=offset)
    fmt = random.choice([
        "%Y-%m-%d %H:%M:%S",
        "%d-%b-%Y %H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
        "%Y%m%dT%H%M%S",
    ])
    return ts.strftime(fmt)


def _ts_date(base: datetime, offset: int) -> str:
    ts = base + timedelta(seconds=offset)
    return ts.strftime("%Y-%m-%d")


def generate_line(base_time: datetime, offset: int) -> str:
    template = random.choice(TEMPLATES)
    return template.format(
        ts_short=_ts_short(base_time, offset),
        ts_long=_ts_long(base_time, offset),
        ts_date=_ts_date(base_time, offset),
        tool=random.choice(TOOLS),
        wafer=random.choice(WAFERS),
        lot=random.choice(LOTS),
        recipe=random.choice(RECIPES),
        step=random.choice(STEPS),
        op=random.choice(OPERATORS),
        gas=random.choice(GASES),
        flow=random.randint(10, 500),
        total_flow=random.randint(200, 2000),
        pressure=random.randint(1, 500),
        temp=random.randint(200, 900),
        actual_temp=random.randint(200, 950),
        delta=round(random.uniform(0.5, 15.0), 1),
        thickness=round(random.uniform(5.0, 500.0), 1),
        target_thick=round(random.uniform(50.0, 300.0), 1),
        uniformity=round(random.uniform(85.0, 99.9), 1),
        reflected=random.randint(5, 100),
        rf_power=random.randint(50, 3000),
        bias=random.randint(10, 500),
        particle_count=random.randint(0, 200),
        prev_particles=random.randint(0, 50),
        endpoint=random.randint(20, 500),
        intensity_drop=round(random.uniform(10.0, 80.0), 1),
        run_num=random.randint(1000, 9999),
        result=random.choice(["PASS", "FAIL", "MARGINAL"]),
        errcode=random.choice(["TMP_90", "GAS_12", "VAC_07", "RF_33", "CHK_01"]),
        eng_num=random.randint(1000, 9999),
        downtime=random.randint(4, 48),
        pn=f"{random.randint(100, 999)}-{random.randint(10, 99)}",
        leak_rate=round(random.uniform(0.1, 9.9), 1),
        vibration=round(random.uniform(0.5, 3.5), 1),
        valve_num=random.randint(1, 16),
        timeout=random.randint(100, 5000),
        retry=random.randint(1, 3),
        resistivity=round(random.uniform(15.0, 18.5), 1),
        he_pressure=round(random.uniform(5.0, 15.0), 1),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate unstructured semiconductor log data.")
    parser.add_argument("-o", "--output", type=Path, default=Path("scripts/unstructured_fab.log"))
    parser.add_argument("-n", "--lines", type=int, default=60)
    args = parser.parse_args()

    base_time = datetime(2026, 3, 7, 6, 0, 0, tzinfo=timezone.utc)
    entries: list[str] = []

    for i in range(args.lines):
        offset = i * random.randint(5, 120)
        entries.append(generate_line(base_time, offset))

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text("\n".join(entries) + "\n", encoding="utf-8")
    print(f"Wrote {len(entries)} entries to {args.output}")


if __name__ == "__main__":
    main()
