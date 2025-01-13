import { string, z } from "zod";
import { randomUUID } from "crypto";
import config from "./config";

export const ComfyNodeSchema = z.object({
  inputs: z.any(),
  class_type: z.string(),
  _meta: z.any().optional(),
});

export type ComfyNode = z.infer<typeof ComfyNodeSchema>;

export const PromptRequestSchema = z.object({
  prompt: z.record(ComfyNodeSchema),
  id: z
    .string()
    .optional()
    .default(() => randomUUID()),
  webhook: z.string().optional(),
});

export type PromptRequest = z.infer<typeof PromptRequestSchema>;

export const PromptResponseSchema = z.object({
  id: z.string(),
  // prompt: z.record(ComfyNodeSchema),
  images: z.array(z.any()).optional(), // avoid using z.string().base64(), if base64 image is too big, cause stack overflow
  webhook: z.string().optional(),
  status: z.enum(["ok"]).optional(),
});

export type PromptResponse = z.infer<typeof PromptResponseSchema>;

export const PromptErrorResponseSchema = z.object({
  error: z.string(),
  location: z.string().optional(),
});

export type PromptErrorResponse = z.infer<typeof PromptErrorResponseSchema>;

export const WorkflowSchema = z.object({
  RequestSchema: z.object({}),
  generateWorkflow: z.function(),
});

export interface Workflow {
  RequestSchema: z.ZodObject<any, any>;
  generateWorkflow: (input: any) => Record<string, ComfyNode>;
  description?: string;
  summary?: string;
}

export function isWorkflow(obj: any): obj is Workflow {
  return "RequestSchema" in obj && "generateWorkflow" in obj;
}

export interface WorkflowTree {
  [key: string]: WorkflowTree | Workflow;
}

export const WorkflowRequestSchema = z.object({
  id: z
    .string()
    .optional()
    .default(() => randomUUID()),
  input: z.record(z.any()),
  webhook: z.string().optional(),
});

export type WorkflowRequest = z.infer<typeof WorkflowRequestSchema>;

export const WorkflowResponseSchema = z.object({
  id: z.string(),
  // input: z.record(z.any()),
  // prompt: z.record(ComfyNodeSchema),
  images: z.array(z.any()).optional(),
  webhook: z.string().optional(),
  status: z.enum(["ok"]).optional(),
});

// Now define the schema after allWorkflows is populated
export const QueueRequestSchema = z.object({
  workflowRoute: z.string(),
  workflowInput: z.object({
    id: z 
      .string()
      .optional()
      .default(() => randomUUID()),
    input: z.record(z.any()),
    webhook: z.string().optional(),
  }), // Changed from WorkflowRequestSchema since inputs vary by workflow
  api_version: z.any(),
});
  
export type QueueRequest = z.infer<typeof QueueRequestSchema>;