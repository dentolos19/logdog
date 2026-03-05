import { API_URL } from "@/environment";
import { createFetch, createSchema } from "@better-fetch/fetch";
import z from "zod";

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
