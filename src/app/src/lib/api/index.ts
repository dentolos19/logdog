import { API_URL } from "@/environment";
import { createFetch, createSchema } from "@better-fetch/fetch";
import z from "zod";

const logTableColumnSchema = z.object({
  name: z.string(),
  type: z.string(),
  not_null: z.boolean(),
  default_value: z.string().nullable(),
  primary_key: z.boolean(),
});

const logGroupTableSchema = z.object({
  id: z.string(),
  name: z.string(),
  columns: z.array(logTableColumnSchema),
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
});

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
  const { data, error } = await $fetch("@put/logs/:id", { body: { name }, params: { id } });
  if (error) throw new Error(error.message ?? "Request failed.");
  return data!;
};

export const deleteLog = async (id: string) => {
  const { data, error } = await $fetch("@delete/logs/:id", { params: { id } });
  if (error) throw new Error(error.message ?? "Request failed.");
};
