import os, shutil, sys, zipfile, tarfile, gzip, bz2, lzma, re, json, base64, tempfile, subprocess
from pathlib import Path
from typing import Optional, Tuple, List
from dataclasses import dataclass
from enum import Enum

try:
    from openai import OpenAI
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    OpenAI = None

try:
    from Evtx.Evtx import Evtx
except ImportError:
    Evtx = None

try:
    import py7zr
except ImportError:
    py7zr = None

try:
    import rarfile
except ImportError:
    rarfile = None

try:
    import msgpack
except ImportError:
    msgpack = None

try:
    import fastavro
except ImportError:
    fastavro = None

try:
    from scapy.all import rdpcap
except ImportError:
    rdpcap = None


class SqlType(str, Enum):
    INTEGER = "INTEGER"
    REAL = "REAL"
    TEXT = "TEXT"
    BLOB = "BLOB"
    BOOLEAN = "BOOLEAN"
    DATETIME = "DATETIME"
    JSON = "JSON"


class ColumnKind(str, Enum):
    BASELINE = "BASELINE"
    EXTRACTED = "EXTRACTED"
    COMPUTED = "COMPUTED"
    METADATA = "METADATA"
    CUSTOM = "CUSTOM"


@dataclass
class InferredColumn:
    name: str
    sql_type: SqlType
    description: str
    nullable: bool = True
    kind: ColumnKind = ColumnKind.BASELINE


SUPPORTED_EXTENSIONS = {
    ".csv",
    ".tsv",
    ".json",
    ".parquet",
    ".xml",
    ".yaml",
    ".yml",
    ".log",
    ".txt",
    ".out",
    ".bin",
    ".dat",
    ".blob",
    ".wal",
    ".blg",
    ".pcap",
    ".pcapng",
    ".cap",
    ".pb",
    ".thrift",
    ".avro",
    ".msgpack",
    ".evtx",
    ".evt",
    ".dmp",
    ".core",
    ".mdmp",
}

ARCHIVE_EXTENSIONS = {".gz", ".bz2", ".xz", ".lzma", ".zip", ".tar", ".7z", ".rar"}


def validate_file(file_path: str) -> Tuple[bool, str]:
    p = Path(file_path)
    if not p.exists():
        return False, f"Not found: {file_path}"
    if not p.is_file():
        return False, f"Not a file: {file_path}"
    if p.suffix.lower() not in SUPPORTED_EXTENSIONS:
        return False, f"Unsupported: {p.suffix}"
    if not os.access(p, os.R_OK):
        return False, "Not readable"
    if p.stat().st_size == 0:
        return False, "Empty"
    return True, str(p)


def load_file(file_path: str) -> Optional[bytes]:
    is_valid, msg = validate_file(file_path)
    if not is_valid:
        print(f"[ERROR] {msg}")
        return None
    try:
        return Path(file_path).read_bytes()
    except Exception as e:
        print(f"[ERROR] {e}")
        return None


def detect_archive_type(file_path: str) -> Optional[str]:
    ext = Path(file_path).suffix.lower()
    return {
        ".zip": "zip",
        ".tar": "tar",
        ".gz": "gzip",
        ".bz2": "bzip2",
        ".xz": "xz",
        ".lzma": "xz",
        ".7z": "7z",
        ".rar": "rar",
    }.get(ext)


def extract_archive(archive_path: str, output_dir: Optional[str] = None) -> Tuple[bool, str, List[str]]:
    if not Path(archive_path).exists():
        return False, "Archive not found", []

    arch_type = detect_archive_type(archive_path)
    if not arch_type:
        return False, "Unknown type", []

    output_dir = output_dir or tempfile.mkdtemp()
    try:
        if arch_type == "zip":
            with zipfile.ZipFile(archive_path) as z:
                z.extractall(output_dir)
                return True, output_dir, z.namelist()
        elif arch_type == "tar":
            with tarfile.open(archive_path) as t:
                t.extractall(output_dir)
                return True, output_dir, t.getnames()
        elif arch_type in ["gzip", "bzip2", "xz"]:
            out = Path(output_dir) / Path(archive_path).stem
            f_cls = gzip.open if arch_type == "gzip" else (bz2.open if arch_type == "bzip2" else lzma.open)
            with f_cls(archive_path, "rb") as f:
                out.write_bytes(f.read())
            return True, output_dir, [str(out)]
        elif arch_type == "7z":
            if not py7zr:
                return False, "py7zr not installed", []
            with py7zr.SevenZipFile(archive_path) as z:
                z.extractall(output_dir)
                return True, output_dir, z.getnames()
        elif arch_type == "rar":
            if not rarfile:
                return False, "rarfile not installed", []
            with rarfile.RarFile(archive_path) as r:
                r.extractall(output_dir)
                return True, output_dir, r.namelist()
        return False, "Unsupported", []
    except Exception as e:
        return False, str(e), []


_DOTNET_PARSER = None


def _find_dotnet_parser() -> Optional[Path]:
    global _DOTNET_PARSER
    if _DOTNET_PARSER is False:
        return None
    if _DOTNET_PARSER:
        return _DOTNET_PARSER
    for p in [
        Path(__file__).parent / "LogParser" / "bin" / "Release" / "net6.0" / "LogParser.exe",
        Path(__file__).parent / "LogParser" / "bin" / "Debug" / "net6.0" / "LogParser.exe",
    ]:
        if p.exists():
            _DOTNET_PARSER = p
            return p
    _DOTNET_PARSER = False
    return None


def _try_dotnet(file_path: str) -> str:
    parser = _find_dotnet_parser()
    if not parser:
        return ""
    try:
        out = Path(tempfile.gettempdir()) / (Path(file_path).stem + "_parsed.json")
        subprocess.run(
            [str(parser), file_path, str(out)],
            check=True,
            capture_output=True,
            timeout=30,
        )
        if out.exists():
            return out.read_text(errors="ignore")
    except:
        pass
    return ""


def decode_msgpack(content: bytes) -> str:
    """Decode msgpack binary data to readable format."""
    if not msgpack:
        return base64.b64encode(content).decode("ascii")
    try:
        data = msgpack.unpackb(content, raw=False)
        return json.dumps(data, default=str, indent=2)
    except Exception as e:
        return f"[msgpack decode error: {str(e)}]\n{base64.b64encode(content).decode('ascii')}"


def decode_avro(file_path: str) -> str:
    """Decode Avro binary data to readable format."""
    if not fastavro:
        return base64.b64encode(Path(file_path).read_bytes()).decode("ascii")
    try:
        records = []
        with open(file_path, "rb") as f:
            reader = fastavro.reader(f)
            for i, record in enumerate(reader):
                if i >= 100:  # limit to first 100 records
                    break
                records.append(record)
        return "\n".join(json.dumps(r, default=str) for r in records)
    except Exception as e:
        return f"[avro decode error: {str(e)}]\n{base64.b64encode(Path(file_path).read_bytes()).decode('ascii')}"


def decode_pcap(file_path: str) -> str:
    """Decode PCAP/PCAPNG packet capture data to readable format."""
    if not rdpcap:
        return base64.b64encode(Path(file_path).read_bytes()).decode("ascii")
    try:
        packets = rdpcap(file_path)
        results = []
        for i, pkt in enumerate(packets[:100]):  # limit to first 100 packets
            pkt_info = f"Packet {i + 1}: "
            if hasattr(pkt, "summary"):
                pkt_info += pkt.summary()
            else:
                pkt_info += str(pkt)[:200]
            results.append(pkt_info)
        return "\n".join(results)
    except Exception as e:
        return f"[pcap decode error: {str(e)}]\n{base64.b64encode(Path(file_path).read_bytes()).decode('ascii')}"


def normalize_content(content: bytes, file_path: str) -> str:
    ext = Path(file_path).suffix.lower()
    text = content.decode("utf-8", errors="ignore")

    if ext in [".json", ".jsonl"]:
        try:
            data = json.loads(text)
            return "\n".join(json.dumps(x) for x in (data if isinstance(data, list) else [data]))
        except:
            pass
    elif ext in [".csv", ".tsv"]:
        return "\n".join(l.strip() for l in text.splitlines()[:5000] if l.strip())
    elif ext == ".xml":
        return text if "<?xml" in text[:100] else '<?xml version="1.0"?>\n' + text
    elif ext in [".yaml", ".yml"]:
        return text.replace("\t", "  ")
    elif ext in [".evt", ".evtx"]:
        if Evtx:
            try:
                return "\n".join(r.xml() for r in Evtx(file_path).records())
            except:
                pass
        parsed = _try_dotnet(file_path)
        return parsed if parsed else content.hex()
    elif ext in [".bin", ".dat", ".blob"]:
        parsed = _try_dotnet(file_path)
        return parsed if parsed else content.hex()
    elif ext in [".msgpack"]:
        return decode_msgpack(content)
    elif ext in [".avro"]:
        return decode_avro(file_path)
    elif ext in [".pcap", ".pcapng", ".cap"]:
        return decode_pcap(file_path)
    elif ext in [".wal", ".blg", ".dmp", ".mdmp", ".core"]:
        return content.hex()
    elif ext in [".pb", ".thrift"]:
        return base64.b64encode(content).decode("ascii")
    return text


def split_by_type(content: str, file_path: str) -> List[Tuple[str, dict]]:
    ext = Path(file_path).suffix.lower()
    fname = Path(file_path).name
    chunks = []

    if ext in [".json", ".jsonl"]:
        for i, line in enumerate(content.strip().split("\n")):
            if line.strip():
                chunks.append((line, {"source": fname, "type": "json", "line": i + 1}))
    elif ext in [".csv", ".tsv"]:
        lines = content.split("\n")
        header = lines[0] if lines else ""
        for i, line in enumerate(lines[1:], 1):
            if line.strip():
                chunks.append(
                    (
                        line,
                        {"source": fname, "type": "csv", "line": i, "header": header},
                    )
                )
    elif ext == ".xml":
        for i, elem in enumerate(re.findall(r"<[^>]+>[^<]*</[^>]+>", content)):
            chunks.append((elem, {"source": fname, "type": "xml", "element": i + 1}))
    elif ext in [".yaml", ".yml"]:
        for i, doc in enumerate(content.split("---")):
            if doc.strip():
                chunks.append((doc, {"source": fname, "type": "yaml", "doc": i + 1}))
    elif ext == ".parquet":
        try:
            import pandas as pd

            for i, row in pd.read_parquet(file_path).iterrows():
                chunks.append(
                    (
                        str(row.to_dict()),
                        {"source": fname, "type": "parquet", "row": i + 1},
                    )
                )
        except:
            chunks.append((content[:10000], {"source": fname, "type": "parquet"}))
    else:
        for i, line in enumerate(content.split("\n")):
            if line.strip():
                chunks.append((line, {"source": fname, "type": "log", "line": i + 1}))

    return chunks


class AIClient:
    BASELINE_COLS = [
        InferredColumn(
            name="id",
            sql_type=SqlType.INTEGER,
            description="Auto-incrementing primary key for each parsed record.",
            nullable=False,
            kind=ColumnKind.BASELINE,
        ),
        InferredColumn(
            name="timestamp",
            sql_type=SqlType.TEXT,
            description="Normalized ISO-8601 timestamp of the log event, if detectable.",
            kind=ColumnKind.BASELINE,
        ),
        InferredColumn(
            name="timestamp_raw",
            sql_type=SqlType.TEXT,
            description="Original timestamp string exactly as it appeared in the log.",
            kind=ColumnKind.BASELINE,
        ),
        InferredColumn(
            name="source",
            sql_type=SqlType.TEXT,
            description="Identifier for the source of the log (e.g., filename, hostname, service name).",
            kind=ColumnKind.BASELINE,
        ),
        InferredColumn(
            name="source_type",
            sql_type=SqlType.TEXT,
            description="Category of the source (e.g., 'file', 'stream', 'api').",
            kind=ColumnKind.BASELINE,
        ),
        InferredColumn(
            name="log_level",
            sql_type=SqlType.TEXT,
            description="Severity level of the log entry (e.g., INFO, WARN, ERROR).",
            kind=ColumnKind.BASELINE,
        ),
        InferredColumn(
            name="event_type",
            sql_type=SqlType.TEXT,
            description="Classified type of the event (e.g., 'request', 'error', 'metric').",
            kind=ColumnKind.BASELINE,
        ),
        InferredColumn(
            name="message",
            sql_type=SqlType.TEXT,
            description="Primary human-readable message content of the log entry.",
            kind=ColumnKind.BASELINE,
        ),
        InferredColumn(
            name="raw_text",
            sql_type=SqlType.TEXT,
            description="Complete original text of the log record, preserved for traceability.",
            nullable=False,
            kind=ColumnKind.BASELINE,
        ),
        InferredColumn(
            name="record_group_id",
            sql_type=SqlType.TEXT,
            description="Identifier linking related records from the same multiline cluster or transaction.",
            kind=ColumnKind.BASELINE,
        ),
        InferredColumn(
            name="line_start",
            sql_type=SqlType.INTEGER,
            description="1-based line number where this record starts in the source file.",
            kind=ColumnKind.BASELINE,
        ),
        InferredColumn(
            name="line_end",
            sql_type=SqlType.INTEGER,
            description="1-based line number where this record ends in the source file.",
            kind=ColumnKind.BASELINE,
        ),
        InferredColumn(
            name="parse_confidence",
            sql_type=SqlType.REAL,
            description="Confidence score (0.0-1.0) of how accurately this record was parsed.",
            kind=ColumnKind.BASELINE,
        ),
        InferredColumn(
            name="schema_version",
            sql_type=SqlType.TEXT,
            description="Version of the schema used to parse this record.",
            kind=ColumnKind.BASELINE,
        ),
        InferredColumn(
            name="additional_data",
            sql_type=SqlType.TEXT,
            description="JSON object containing any extra fields that did not map to named columns.",
            kind=ColumnKind.BASELINE,
        ),
    ]

    def __init__(self, api_key: Optional[str] = None, model: str = "anthropic/claude-3.5-sonnet"):
        self.api_key = api_key or os.getenv("OPENROUTER_API_KEY")
        self.model = os.getenv("OPENROUTER_MODEL") or model
        self.client = None
        self.baseline_columns = self.BASELINE_COLS

        if not self.api_key:
            print("[ERROR] OPENROUTER_API_KEY not set")
            return
        if not OpenAI:
            print("[ERROR] openai not installed")
            return
        try:
            self.client = OpenAI(
                api_key=self.api_key,
                base_url="https://openrouter.ai/api/v1",
                default_headers={
                    "HTTP-Referer": "http://localhost",
                    "X-Title": "Log Pipeline",
                },
            )
            print(f"[AI] Connected (model: {self.model})")
        except Exception as e:
            print(f"[ERROR] {e}")

    def generate_comment(self, record: dict) -> str:
        """Generate AI comment about a log record."""
        if not self.client or not record:
            return ""
        try:
            msg = record.get("message", "")
            level = record.get("log_level", "INFO")
            event = record.get("event_type", "")
            prompt = f"Log Level: {level}\nEvent Type: {event}\nMessage: {msg}\n\nBriefly comment (1 line) on this log entry."
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=100,
                temperature=0.1,
            )
            return response.choices[0].message.content.strip()
        except:
            return ""

    def process_chunk(self, content: str, metadata: dict, line_num: int = 1) -> Optional[dict]:
        if not self.client:
            return None
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "user",
                        "content": f"Analyze log:\n{content}\n\nReturn JSON with: timestamp, log_level, event_type, message, parse_confidence (0-1)",
                    }
                ],
                max_tokens=300,
                temperature=0.1,
            )
            text = response.choices[0].message.content.strip()
            try:
                ai_data = json.loads(text)
            except:
                import re

                json_str = re.search(r"\{.*\}", text, re.DOTALL)
                ai_data = json.loads(json_str.group()) if json_str else {}

            extra_data = {
                k: v
                for k, v in ai_data.items()
                if k
                not in [
                    "timestamp",
                    "log_level",
                    "event_type",
                    "message",
                    "parse_confidence",
                ]
            }

            return {
                col.name: (
                    ai_data.get("timestamp")
                    if col.name == "timestamp"
                    else content[:50]
                    if col.name == "timestamp_raw"
                    else metadata.get("source", "unknown")
                    if col.name == "source"
                    else metadata.get("type", "unknown")
                    if col.name == "source_type"
                    else ai_data.get("log_level", "INFO")
                    if col.name == "log_level"
                    else ai_data.get("event_type", "log")
                    if col.name == "event_type"
                    else ai_data.get("message", content[:200])
                    if col.name == "message"
                    else content
                    if col.name == "raw_text"
                    else line_num
                    if col.name == "line_start"
                    else line_num
                    if col.name == "line_end"
                    else ai_data.get("parse_confidence", 0.5)
                    if col.name == "parse_confidence"
                    else "1.0"
                    if col.name == "schema_version"
                    else json.dumps(extra_data)
                    if col.name == "additional_data"
                    else None
                )
                for col in self.BASELINE_COLS
            }
        except:
            return None


def process_log_file(file_path: str, max_records: int = 10) -> List[dict]:
    is_valid, msg = validate_file(file_path)
    if not is_valid:
        print(f"[ERROR] {msg}")
        return []

    content_bytes = load_file(file_path)
    if not content_bytes:
        return []

    content_str = normalize_content(content_bytes, file_path)
    chunks = split_by_type(content_str, file_path)

    ai = AIClient()
    if not ai.client:
        return []

    results = []
    for i, (content, metadata) in enumerate(chunks[:max_records]):
        record = ai.process_chunk(content, metadata, line_num=i + 1)
        if record:
            results.append(record)
            print(f"  {i + 1}. ✓")
        else:
            print(f"  {i + 1}. ✗")

    print(f"\n{len(results)} records processed\n")
    return results


def run_pipeline(file_name: str, max_records: int = 10):
    """Process log file and display results as dataframe with all baseline columns."""
    results = process_log_file(file_name, max_records)

    if not results:
        print("[ERROR] No records processed")
        return None

    try:
        import pandas as pd

        df = pd.DataFrame(results)
        print(df)
        return df
    except ImportError:
        print("[ERROR] pandas not installed. Install with: pip install pandas")
        return results


if __name__ == "__main__":
    file_name = "test_logs.csv"
    max_records = 10
    run_pipeline(file_name, max_records)
