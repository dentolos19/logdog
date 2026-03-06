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
