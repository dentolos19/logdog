import { schema } from "@/lib/api";
import z from "zod";

export type User = z.infer<(typeof schema.schema)["/auth/me"]["output"]>;
