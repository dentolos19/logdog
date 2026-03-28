"""
Step 4: Fuzzy Header Matching
=============================
Maps extracted field names to canonical headers using:
  - Exact match lookup
  - Alias dictionary
  - Levenshtein distance (fuzzy matching)

The canonical schema is tailored to semiconductor manufacturing
log fields (equipment IDs, recipe parameters, sensor data, etc.)
"""

from dataclasses import dataclass
from typing import Optional


# ---------------------------------------------------------------------------
# Canonical field names and their known aliases
# ---------------------------------------------------------------------------
CANONICAL_ALIASES: dict[str, list[str]] = {
    # Identifiers
    "control_job_id": ["CtrlJobID", "CJOB", "ctrl_job_id", "controlJobId", "cjob_id"],
    "equipment_id": [
        "EquipmentID",
        "EQP",
        "equip_id",
        "machineID",
        "MachineID",
        "eqp_id",
        "tool_id",
    ],
    "process_job_id": ["PRJobID", "PRJOB", "pr_job_id", "processJobId", "prjob_id"],
    "lot_id": ["LotID", "LOT", "lot", "lotId", "lot_number"],
    "wafer_id": ["WaferID", "WFR", "wafer", "waferId", "wafer_number", "wfr_id"],
    "module_id": ["ModuleID", "MOD", "module", "moduleId", "mod_id"],
    "recipe_id": ["RecipeID", "RCP", "recipe", "recipeId", "rcp_id", "ModuleRecipeID"],
    "recipe_step_id": ["RecipeStepID", "step_id", "stepId", "step", "STEP"],
    "sensor_id": ["SensorID", "sensor", "sensorId", "SENSOR"],
    "carrier_id": ["CarrierID", "carrier", "carrierId", "CARRIER"],
    "slot_id": ["SlotID", "slot", "slotId", "SLOT"],
    "port_id": ["PortID", "port", "portId", "PORT"],
    "site_id": ["Site", "site", "siteId", "SITE"],
    "customer_id": ["Customer", "customer", "customerId", "CUST"],
    # Timestamps
    "timestamp": ["ts", "time", "datetime", "date_time", "log_time", "event_time"],
    "start_time": [
        "CtrlJobStartTime",
        "WaferStartTime",
        "RecipeStartTime",
        "startTime",
        "start_ts",
        "begin_time",
    ],
    "end_time": [
        "CtrlJobEndTime",
        "WaferEndTime",
        "RecipeEndTime",
        "endTime",
        "end_ts",
        "finish_time",
    ],
    # Process parameters
    "duration": ["Duration", "elapsed", "elapsed_time", "process_time", "ProcessTime"],
    "pressure": ["PressureSetpoint", "pressure_sp", "Pressure", "chamber_pressure"],
    "temperature": ["Temperature", "temp", "TemperatureSetpoint", "temp_sp"],
    "rf_power": [
        "HFPowerSetpoint",
        "LFPowerSetpoint",
        "rf_power",
        "RFPower",
        "StationRF",
        "Station1RF",
    ],
    "gas_flow": [
        "N2_A",
        "N2_B",
        "Ar_B",
        "Ar_C",
        "SiH4_A",
        "NF3_C",
        "NH3_B",
        "He_B",
        "N2O_B",
        "N2_D",
        "N2_D2",
    ],
    # Recipe metadata
    "recipe_name": ["RecipeStepName", "recipe_name", "recipeName", "step_name"],
    "recipe_type": ["RecipeType", "recipe_type", "recipeType", "FileType"],
    "module_type": ["ModuleType", "module_type", "moduleType", "chamber_type"],
    "platform": ["Platform", "platform", "tool_platform"],
    "application": ["PMApplication", "application", "app", "process_app"],
    # File / version metadata
    "file_type": ["FileType", "file_type", "fileType"],
    "software_version": [
        "FileGeneratorSoftwareVersion",
        "Version",
        "version",
        "sw_version",
        "software_ver",
        "DataFileVersion",
        "ParquetSchemaVersion",
        "JsonSchemaVersion",
    ],
    # Sensor data
    "sensor_value": ["SVID|Monitor|", "sensor_val", "reading", "measurement"],
    "sensor_unit": ["Unit", "unit", "units", "measurement_unit"],
    "data_type": ["DataType", "Type", "type", "data_type", "value_type"],
    # Log level / status
    "log_level": ["level", "severity", "loglevel", "log_level", "LEVEL"],
    "source": ["source", "origin", "src", "component", "service"],
    "message": ["message", "msg", "text", "description", "log_message"],
}

# Build reverse lookup: alias → canonical
_ALIAS_TO_CANONICAL: dict[str, str] = {}
for canonical, aliases in CANONICAL_ALIASES.items():
    _ALIAS_TO_CANONICAL[canonical.lower()] = canonical
    for alias in aliases:
        _ALIAS_TO_CANONICAL[alias.lower()] = canonical


# ---------------------------------------------------------------------------
# Levenshtein distance
# ---------------------------------------------------------------------------
def _levenshtein(s1: str, s2: str) -> int:
    """Compute the Levenshtein edit distance between two strings."""
    if len(s1) < len(s2):
        return _levenshtein(s2, s1)
    if len(s2) == 0:
        return len(s1)

    prev_row = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        curr_row = [i + 1]
        for j, c2 in enumerate(s2):
            cost = 0 if c1 == c2 else 1
            curr_row.append(
                min(
                    curr_row[j] + 1,
                    prev_row[j + 1] + 1,
                    prev_row[j] + cost,
                )
            )
        prev_row = curr_row
    return prev_row[-1]


def _jaro_similarity(s1: str, s2: str) -> float:
    """Compute Jaro similarity between two strings (0.0–1.0)."""
    if s1 == s2:
        return 1.0
    len_s1, len_s2 = len(s1), len(s2)
    if len_s1 == 0 or len_s2 == 0:
        return 0.0

    match_dist = max(len_s1, len_s2) // 2 - 1
    s1_matches = [False] * len_s1
    s2_matches = [False] * len_s2
    matches = 0
    transpositions = 0

    for i in range(len_s1):
        start = max(0, i - match_dist)
        end = min(i + match_dist + 1, len_s2)
        for j in range(start, end):
            if s2_matches[j] or s1[i] != s2[j]:
                continue
            s1_matches[i] = True
            s2_matches[j] = True
            matches += 1
            break

    if matches == 0:
        return 0.0

    k = 0
    for i in range(len_s1):
        if not s1_matches[i]:
            continue
        while not s2_matches[k]:
            k += 1
        if s1[i] != s2[k]:
            transpositions += 1
        k += 1

    return (matches / len_s1 + matches / len_s2 + (matches - transpositions / 2) / matches) / 3


# ---------------------------------------------------------------------------
# Match result
# ---------------------------------------------------------------------------
@dataclass
class FuzzyMatch:
    original_key: str
    canonical_key: str
    similarity: float  # 0.0–1.0
    match_method: str  # 'exact', 'alias', 'fuzzy'


# ---------------------------------------------------------------------------
# Fuzzy Matcher
# ---------------------------------------------------------------------------
class FuzzyMatcher:
    """Map raw field names to canonical headers."""

    def __init__(
        self,
        custom_aliases: Optional[dict[str, list[str]]] = None,
        similarity_threshold: float = 0.75,
    ):
        self.threshold = similarity_threshold
        self._aliases = dict(_ALIAS_TO_CANONICAL)
        if custom_aliases:
            for canonical, aliases in custom_aliases.items():
                self._aliases[canonical.lower()] = canonical
                for alias in aliases:
                    self._aliases[alias.lower()] = canonical

    def match_key(self, raw_key: str) -> Optional[FuzzyMatch]:
        """Match a single raw key to its canonical form."""
        lower = raw_key.lower().strip()

        # 1. Exact match
        if lower in self._aliases:
            return FuzzyMatch(
                original_key=raw_key,
                canonical_key=self._aliases[lower],
                similarity=1.0,
                match_method="exact",
            )

        # 2. Fuzzy match against all known aliases
        best_match: Optional[FuzzyMatch] = None
        best_score = 0.0

        for alias, canonical in self._aliases.items():
            score = _jaro_similarity(lower, alias)
            if score > best_score and score >= self.threshold:
                best_score = score
                best_match = FuzzyMatch(
                    original_key=raw_key,
                    canonical_key=canonical,
                    similarity=score,
                    match_method="fuzzy",
                )

        return best_match

    def match_keys(self, raw_keys: list[str]) -> dict[str, FuzzyMatch]:
        """Match a list of raw keys, returning a mapping."""
        results: dict[str, FuzzyMatch] = {}
        for key in raw_keys:
            m = self.match_key(key)
            if m:
                results[key] = m
        return results

    def remap_dict(self, data: dict) -> dict:
        """Remap a dict's keys to canonical form where matches are found."""
        out: dict = {}
        for key, value in data.items():
            m = self.match_key(key)
            if m:
                out[m.canonical_key] = value
            else:
                out[key] = value  # keep original if no match
        return out
