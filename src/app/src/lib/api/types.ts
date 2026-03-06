import { schema } from "@/lib/api";
import z from "zod";

export type User = z.infer<(typeof schema.schema)["/auth/me"]["output"]>;

export interface LogTableColumn {
  name: string;
  type: string;
  not_null: boolean;
  default_value: string | null;
  primary_key: boolean;
}

export interface LogGroupTable {
  id: string;
  name: string;
  columns: LogTableColumn[];
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
