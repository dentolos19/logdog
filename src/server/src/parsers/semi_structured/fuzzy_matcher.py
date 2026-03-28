from dataclasses import dataclass

CANONICAL_ALIASES: dict[str, list[str]] = {
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
    "recipe_name": ["RecipeStepName", "recipe_name", "recipeName", "step_name"],
    "recipe_type": ["RecipeType", "recipe_type", "recipeType", "FileType"],
    "module_type": ["ModuleType", "module_type", "moduleType", "chamber_type"],
    "platform": ["Platform", "platform", "tool_platform"],
    "application": ["PMApplication", "application", "app", "process_app"],
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
    "sensor_value": ["SVID|Monitor|", "sensor_val", "reading", "measurement"],
    "sensor_unit": ["Unit", "unit", "units", "measurement_unit"],
    "data_type": ["DataType", "Type", "type", "data_type", "value_type"],
    "log_level": ["level", "severity", "loglevel", "log_level", "LEVEL"],
    "source": ["source", "origin", "src", "component", "service"],
    "message": ["message", "msg", "text", "description", "log_message"],
}

_ALIAS_TO_CANONICAL: dict[str, str] = {}
for canonical, aliases in CANONICAL_ALIASES.items():
    _ALIAS_TO_CANONICAL[canonical.lower()] = canonical
    for alias in aliases:
        _ALIAS_TO_CANONICAL[alias.lower()] = canonical


def _jaro_similarity(left: str, right: str) -> float:
    if left == right:
        return 1.0
    if not left or not right:
        return 0.0

    len_left = len(left)
    len_right = len(right)
    match_distance = max(len_left, len_right) // 2 - 1

    left_matches = [False] * len_left
    right_matches = [False] * len_right

    matches = 0
    transpositions = 0

    for index in range(len_left):
        start = max(0, index - match_distance)
        end = min(index + match_distance + 1, len_right)
        for candidate in range(start, end):
            if right_matches[candidate] or left[index] != right[candidate]:
                continue
            left_matches[index] = True
            right_matches[candidate] = True
            matches += 1
            break

    if matches == 0:
        return 0.0

    k = 0
    for index in range(len_left):
        if not left_matches[index]:
            continue
        while not right_matches[k]:
            k += 1
        if left[index] != right[k]:
            transpositions += 1
        k += 1

    return (matches / len_left + matches / len_right + (matches - transpositions / 2) / matches) / 3


@dataclass
class FuzzyMatch:
    original_key: str
    canonical_key: str
    similarity: float
    match_method: str


class FuzzyMatcher:
    def __init__(self, custom_aliases: dict[str, list[str]] | None = None, similarity_threshold: float = 0.75):
        self.threshold = similarity_threshold
        self._aliases = dict(_ALIAS_TO_CANONICAL)
        if custom_aliases:
            for canonical, aliases in custom_aliases.items():
                self._aliases[canonical.lower()] = canonical
                for alias in aliases:
                    self._aliases[alias.lower()] = canonical

    def match_key(self, raw_key: str) -> FuzzyMatch | None:
        lowered = raw_key.lower().strip()

        if lowered in self._aliases:
            return FuzzyMatch(
                original_key=raw_key, canonical_key=self._aliases[lowered], similarity=1.0, match_method="exact"
            )

        best_match: FuzzyMatch | None = None
        best_score = 0.0
        for alias, canonical in self._aliases.items():
            score = _jaro_similarity(lowered, alias)
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
        result: dict[str, FuzzyMatch] = {}
        for raw_key in raw_keys:
            matched = self.match_key(raw_key)
            if matched is not None:
                result[raw_key] = matched
        return result

    def remap_dict(self, data: dict) -> dict:
        remapped: dict = {}
        for key, value in data.items():
            matched = self.match_key(key)
            if matched is not None:
                remapped[matched.canonical_key] = value
            else:
                remapped[key] = value
        return remapped
