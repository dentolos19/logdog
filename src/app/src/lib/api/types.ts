import type z from "zod";
import type { schema } from "@/lib/api";

export type User = z.infer<(typeof schema.schema)["/auth/me"]["output"]>;

export interface LogTableColumn {
  name: string;
  type: string;
  description: string;
  not_null: boolean;
  default_value: string | null;
  primary_key: boolean;
}

export interface LogGroupTable {
  id: string;
  name: string;
  columns: LogTableColumn[];
  row_count: number;
  is_normalized: boolean;
  created_at: string;
  updated_at: string;
}

export interface LogGroupListItem {
  id: string;
  name: string;
  created_at: string;
  updated_at: string;
}

export interface LogGroup extends LogGroupListItem {
  tables: LogGroupTable[];
}

export interface InferredColumn {
  name: string;
  sql_type: string;
  description: string;
  nullable: boolean;
  kind: string;
  example_values: string[];
}

export interface GeneratedTable {
  table_name: string;
  sqlite_ddl: string;
  columns: InferredColumn[];
  is_normalized: boolean;
  file_id: string | null;
  file_name: string | null;
}

export interface SegmentationResult {
  strategy: string;
  confidence: number;
  rationale: string;
}

export interface FileObservation {
  filename: string;
  line_count: number;
  detected_format: string;
  format_confidence: number;
  segmentation_hint: string;
  sample_size: number;
  warnings: string[];
}

export interface SampleRecord {
  source_file: string;
  line_start: number;
  line_end: number;
  fields: Record<string, unknown>;
}

export interface PreprocessResult {
  id: string;
  log_id: string;
  schema_summary: string;
  schema_version: string;
  table_name: string;
  sqlite_ddl: string;
  columns: InferredColumn[];
  generated_tables: GeneratedTable[];
  segmentation: SegmentationResult;
  sample_records: SampleRecord[];
  file_observations: FileObservation[];
  warnings: string[];
  assumptions: string[];
  confidence: number;
  created_at: string;
}

export interface UploadLogFilesResponse {
  uploaded_files: number;
  process_result: PreprocessResult;
}

export interface FileClassification {
  file_id: string | null;
  filename: string;
  detected_format: string;
  structural_class: string;
  format_confidence: number;
  line_count: number;
  warnings: string[];
}

export interface ClassificationResult {
  schema_version: string;
  dominant_format: string;
  structural_class: string;
  selected_parser_key: string;
  file_classifications: FileClassification[];
  warnings: string[];
  confidence: number;
}

export interface UploadLogFilesV2Response {
  uploaded_files: number;
  process_id: string;
  classification: ClassificationResult;
}

export interface ProcessResultDetails {
  schema_summary: string;
  schema_version: string;
  table_name: string;
  sqlite_ddl: string;
  columns: InferredColumn[];
  generated_tables: GeneratedTable[];
  segmentation: SegmentationResult;
  sample_records: SampleRecord[];
  file_observations: FileObservation[];
  warnings: string[];
  assumptions: string[];
  confidence: number;
}

export interface LogProcess {
  id: string;
  log_id: string;
  status: string;
  error: string | null;
  result: ProcessResultDetails | null;
  created_at: string;
  updated_at: string;
}

export interface ParsedLogRow {
  // Baseline columns — mirrors preprocessor._build_baseline_columns()
  id: string;
  timestamp: string | null;
  timestamp_raw: string | null;
  source: string;
  source_type: string;
  log_level: string;
  event_type: string;
  message: string;
  raw_text: string;
  record_group_id: string | null;
  line_start: number | null;
  line_end: number | null;
  parse_confidence: number;
  schema_version: string;
  additional_data: Record<string, unknown>;
  // Pipeline-only
  raw_hash: string;
  template_id: string | null;
  // Semiconductor-extended
  equipment_id: string | null;
  lot_id: string | null;
  wafer_id: string | null;
  recipe_id: string | null;
  step_id: string | null;
  module_id: string | null;
}

export interface FileParseResult {
  filename: string;
  stages_executed: string[];
  confidence: number;
  format_detected: string | null;
  total_latency_ms: number;
  ai_fallback_used: boolean;
  log_row: ParsedLogRow;
}

export interface LogGroupFile {
  id: string;
  asset_id: string;
  name: string;
  size: number;
  mime_type: string;
  created_at: string;
}

export interface TableRowsResponse {
  columns: string[];
  rows: Record<string, unknown>[];
  total: number;
  page: number;
  page_size: number;
  total_pages: number;
}
