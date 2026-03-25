import { createFetch, createSchema } from "@better-fetch/fetch";
import z from "zod";
import { API_URL } from "@/environment";

const logTableColumnSchema = z.object({
	name: z.string(),
	type: z.string(),
	description: z.string(),
	not_null: z.boolean(),
	default_value: z.string().nullable(),
	primary_key: z.boolean(),
});

const logGroupTableSchema = z.object({
	id: z.string(),
	name: z.string(),
	columns: z.array(logTableColumnSchema),
	row_count: z.number(),
	is_normalized: z.boolean(),
	created_at: z.string(),
	updated_at: z.string(),
});

const logGroupListItemSchema = z.object({
	id: z.string(),
	name: z.string(),
	created_at: z.string(),
	updated_at: z.string(),
});

const logGroupSchema = logGroupListItemSchema.extend({
	tables: z.array(logGroupTableSchema),
});

const inferredColumnSchema = z.object({
	name: z.string(),
	sql_type: z.string(),
	description: z.string(),
	nullable: z.boolean(),
	kind: z.string(),
	example_values: z.array(z.string()),
});

const generatedTableSchema = z.object({
	table_name: z.string(),
	sqlite_ddl: z.string(),
	columns: z.array(inferredColumnSchema),
	is_normalized: z.boolean(),
	file_id: z.string().nullable(),
	file_name: z.string().nullable(),
});

const segmentationResultSchema = z.object({
	strategy: z.string(),
	confidence: z.number(),
	rationale: z.string(),
});

const fileObservationSchema = z.object({
	filename: z.string(),
	line_count: z.number(),
	detected_format: z.string(),
	format_confidence: z.number(),
	segmentation_hint: z.string(),
	sample_size: z.number(),
	warnings: z.array(z.string()),
});

const sampleRecordSchema = z.object({
	source_file: z.string(),
	line_start: z.number(),
	line_end: z.number(),
	fields: z.record(z.string(), z.unknown()),
});

const preprocessResultSchema = z.object({
	id: z.string(),
	log_id: z.string(),
	schema_summary: z.string(),
	schema_version: z.string(),
	table_name: z.string(),
	sqlite_ddl: z.string(),
	columns: z.array(inferredColumnSchema),
	generated_tables: z.array(generatedTableSchema),
	segmentation: segmentationResultSchema,
	sample_records: z.array(sampleRecordSchema),
	file_observations: z.array(fileObservationSchema),
	warnings: z.array(z.string()),
	assumptions: z.array(z.string()),
	confidence: z.number(),
	created_at: z.string(),
});

const fileClassificationSchema = z.object({
	file_id: z.string().nullable(),
	filename: z.string(),
	detected_format: z.string(),
	structural_class: z.string(),
	format_confidence: z.number(),
	line_count: z.number(),
	warnings: z.array(z.string()),
});

const classificationResponseSchema = z.object({
	schema_version: z.string(),
	dominant_format: z.string(),
	structural_class: z.string(),
	selected_parser_key: z.string(),
	file_classifications: z.array(fileClassificationSchema),
	warnings: z.array(z.string()),
	confidence: z.number(),
});

const uploadLogFilesResponseSchema = z.object({
	uploaded_files: z.number(),
	process_id: z.string(),
	classification: classificationResponseSchema,
});

const processResultDetailsSchema = z.object({
	schema_summary: z.string(),
	schema_version: z.string(),
	table_name: z.string(),
	sqlite_ddl: z.string(),
	columns: z.array(inferredColumnSchema),
	generated_tables: z.array(generatedTableSchema),
	segmentation: segmentationResultSchema,
	sample_records: z.array(sampleRecordSchema),
	file_observations: z.array(fileObservationSchema),
	warnings: z.array(z.string()),
	assumptions: z.array(z.string()),
	confidence: z.number(),
});

const processResponseSchema = z.object({
	id: z.string(),
	log_id: z.string(),
	status: z.string(),
	error: z.string().nullable(),
	result: processResultDetailsSchema.nullable(),
	created_at: z.string(),
	updated_at: z.string(),
});

const logGroupFileSchema = z.object({
	id: z.string(),
	asset_id: z.string(),
	name: z.string(),
	size: z.number(),
	mime_type: z.string(),
	created_at: z.string(),
});

const processStatusCountSchema = z.object({
	pending: z.number(),
	classified: z.number(),
	processing: z.number(),
	completed: z.number(),
	failed: z.number(),
});

const dashboardStatsSchema = z.object({
	log_group_count: z.number(),
	total_files: z.number(),
	total_rows: z.number(),
	processes: processStatusCountSchema,
});

export const schema = createSchema({
	"/auth/login": {
		method: "post",
		input: z.object({
			email: z.string().email(),
			password: z.string(),
		}),
		output: z.object({
			id: z.string(),
			email: z.string().email(),
		}),
	},
	"/auth/register": {
		method: "post",
		input: z.object({
			email: z.string().email(),
			password: z.string(),
		}),
		output: z.object({
			id: z.string(),
			email: z.string().email(),
		}),
	},
	"/auth/me": {
		method: "get",
		output: z.object({
			id: z.string(),
			email: z.string().email(),
		}),
	},
	"/auth/refresh": {
		method: "post",
	},
	"/auth/logout": {
		method: "post",
	},
	"/logs": {
		method: "get",
		output: z.array(logGroupListItemSchema),
	},
	"@post/logs": {
		method: "post",
		input: z.object({ name: z.string() }),
		output: logGroupSchema,
	},
	"@get/logs/:id": {
		method: "get",
		output: logGroupSchema,
	},
	"@put/logs/:id": {
		method: "put",
		input: z.object({ name: z.string() }),
		output: logGroupSchema,
	},
	"@delete/logs/:id": {
		method: "delete",
	},
	"@get/logs/:id/processes": {
		method: "get",
		output: z.array(processResponseSchema),
	},
	"@get/logs/:id/files": {
		method: "get",
		output: z.array(logGroupFileSchema),
	},
	"@get/stats": {
		method: "get",
		output: dashboardStatsSchema,
	},
});

export type UploadLogFilesResponse = z.infer<typeof uploadLogFilesResponseSchema>;
export type ClassificationResponse = z.infer<typeof classificationResponseSchema>;
export type FileClassification = z.infer<typeof fileClassificationSchema>;
export type ProcessResponse = z.infer<typeof processResponseSchema>;
export type DashboardStats = z.infer<typeof dashboardStatsSchema>;

export const errorSchema = z.object({
	message: z.string(),
});

export const $fetch = createFetch({
	baseURL: API_URL,
	credentials: "include",
	schema,
	errorSchema,
});

export const getLogs = async () => {
	const { data, error } = await $fetch("@get/logs");
	if (error) throw new Error(error.message ?? "Request failed.");
	return data!;
};

export const getLog = async (id: string) => {
	const { data, error } = await $fetch("@get/logs/:id", { params: { id } });
	if (error) throw new Error(error.message ?? "Request failed.");
	return data!;
};

export const createLog = async (name: string) => {
	const { data, error } = await $fetch("@post/logs", { body: { name } });
	if (error) throw new Error(error.message ?? "Request failed.");
	return data!;
};

export const updateLog = async (id: string, name: string) => {
	const { data, error } = await $fetch("@put/logs/:id", {
		body: { name },
		params: { id },
	});
	if (error) throw new Error(error.message ?? "Request failed.");
	return data!;
};

export const deleteLog = async (id: string) => {
	const { data, error } = await $fetch("@delete/logs/:id", { params: { id } });
	if (error) throw new Error(error.message ?? "Request failed.");
};

export const uploadLogFiles = async (id: string, files: File[]) => {
	const formData = new FormData();
	for (const file of files) {
		formData.append("files", file);
	}

	const response = await fetch(`${API_URL}/logs/${encodeURIComponent(id)}`, {
		method: "POST",
		credentials: "include",
		body: formData,
	});

	if (!response.ok) {
		const body = (await response.json().catch(() => ({ message: "Upload failed." }))) as {
			message?: string;
		};
		throw new Error(body.message ?? "Upload failed.");
	}

	return uploadLogFilesResponseSchema.parse(await response.json());
};

export const getLogProcesses = async (id: string) => {
	const { data, error } = await $fetch("@get/logs/:id/processes", {
		params: { id },
	});
	if (error) throw new Error(error.message ?? "Request failed.");
	return data!;
};

export const getLogFiles = async (id: string) => {
	const { data, error } = await $fetch("@get/logs/:id/files", {
		params: { id },
	});
	if (error) throw new Error(error.message ?? "Request failed.");
	return data!;
};

export const getStats = async () => {
	const { data, error } = await $fetch("@get/stats");
	if (error) throw new Error(error.message ?? "Request failed.");
	return data!;
};

export const testParser = async (files: File[]): Promise<import("./types").FileParseResult[]> => {
	const formData = new FormData();
	for (const file of files) {
		formData.append("files", file);
	}

	const response = await fetch(`${API_URL}/parser/test`, {
		method: "POST",
		credentials: "include",
		body: formData,
	});

	if (!response.ok) {
		const body = (await response.json().catch(() => ({ message: "Parse failed." }))) as {
			message?: string;
		};
		throw new Error(body.message ?? "Parse failed.");
	}

	return response.json() as Promise<import("./types").FileParseResult[]>;
};

export const downloadLogFile = async (logGroupId: string, fileId: string): Promise<Blob> => {
	const response = await fetch(
		`${API_URL}/logs/${encodeURIComponent(logGroupId)}/files/${encodeURIComponent(fileId)}/download`,
		{
			method: "GET",
			credentials: "include",
		},
	);

	if (!response.ok) {
		const body = (await response.json().catch(() => ({ message: "Download failed." }))) as {
			message?: string;
		};
		throw new Error(body.message ?? "Download failed.");
	}

	return response.blob();
};

export const getTableRows = async (
	logGroupId: string,
	tableName: string,
	page: number,
	pageSize: number,
): Promise<import("./types").TableRowsResponse> => {
	const params = new URLSearchParams({
		page: String(page),
		page_size: String(pageSize),
	});
	const response = await fetch(
		`${API_URL}/logs/${encodeURIComponent(logGroupId)}/tables/${encodeURIComponent(tableName)}/rows?${params}`,
		{ method: "GET", credentials: "include" },
	);

	if (!response.ok) {
		const body = (await response.json().catch(() => ({ message: "Failed to load rows." }))) as {
			message?: string;
		};
		throw new Error(body.message ?? "Failed to load rows.");
	}

	return response.json() as Promise<import("./types").TableRowsResponse>;
};
