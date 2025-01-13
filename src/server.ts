import Fastify from "fastify";
import fastifySwagger from "@fastify/swagger";
import fastifySwaggerUI from "@fastify/swagger-ui";
import {
  jsonSchemaTransform,
  serializerCompiler,
  validatorCompiler,
  ZodTypeProvider,
} from "fastify-type-provider-zod";
import { DirectoryWatcher } from "./watcher";
import fsPromises from "fs/promises";
import path from "path";
import { version } from "../package.json";
import config from "./config";
import {
  warmupComfyUI,
  waitForComfyUIToStart,
  launchComfyUI,
  shutdownComfyUI,
  processImage,
  zodToMarkdownTable,
} from "./utils";
import {
  PromptRequestSchema,
  PromptErrorResponseSchema,
  PromptResponseSchema,
  PromptRequest,
  WorkflowResponseSchema,
  WorkflowTree,
  isWorkflow,
  WorkflowRequest,
  WorkflowRequestSchema,
  Workflow,
  QueueRequest,
  QueueRequestSchema,
} from "./types";
import workflows from "./workflows";
import { z } from "zod";
import { randomUUID } from "crypto";
import { create } from "domain";

const outputWatcher = new DirectoryWatcher(config.outputDir);

const server = Fastify({
  bodyLimit: 45 * 1024 * 1024, // 45MB
  logger: true,
});
server.setValidatorCompiler(validatorCompiler);
server.setSerializerCompiler(serializerCompiler);

const modelSchema: any = {};
for (const modelType in config.models) {
  modelSchema[modelType] = z.string().array();
}

const ModelResponseSchema = z.object(modelSchema);
type ModelResponse = z.infer<typeof ModelResponseSchema>;

const modelResponse: ModelResponse = {};
for (const modelType in config.models) {
  modelResponse[modelType] = config.models[modelType].all;
}

const allWorkflows: string[] = [];

server.register(fastifySwagger, {
  openapi: {
    openapi: "3.0.0",
    info: {
      title: "Comfy Wrapper API",
      version,
    },
    servers: [
      {
        url: `{accessDomainName}`,
        description: "Your server",
        variables: {
          accessDomainName: {
            default: `http://localhost:${config.wrapperPort}`,
            description:
              "The domain name of the server, protocol included, port optional",
          },
        },
      },
    ],
  },
  transform: jsonSchemaTransform,
});
server.register(fastifySwaggerUI, {
  routePrefix: "/docs",
  uiConfig: {
    deepLinking: true,
  },
});

server.after(() => {
  const app = server.withTypeProvider<ZodTypeProvider>();
  app.get(
    "/health",
    {
      schema: {
        summary: "Health Probe",
        description: "Check if the server is healthy",
        response: {
          200: z.object({
            version: z.literal(version),
            status: z.literal("healthy"),
          }),
          500: z.object({
            version: z.literal(version),
            status: z.literal("not healthy"),
          }),
        },
      },
    },
    async (request, reply) => {
      // 200 if ready, 500 if not
      if (warm) {
        return reply.code(200).send({ version, status: "healthy" });
      }
      return reply.code(500).send({ version, status: "not healthy" });
    }
  );

  app.get(
    "/ready",
    {
      schema: {
        summary: "Readiness Probe",
        description: "Check if the server is ready to serve traffic",
        response: {
          200: z.object({
            version: z.literal(version),
            status: z.literal("ready"),
          }),
          500: z.object({
            version: z.literal(version),
            status: z.literal("not ready"),
          }),
        },
      },
    },
    async (request, reply) => {
      if (warm) {
        return reply.code(200).send({ version, status: "ready" });
      }
      return reply.code(500).send({ version, status: "not ready" });
    }
  );

  app.get(
    "/models",
    {
      schema: {
        summary: "List Models",
        description:
          "List all available models. This is from the contents of the models directory.",
        response: {
          200: ModelResponseSchema,
        },
      },
    },
    async (request, reply) => {
      return modelResponse;
    }
  );

  app.post<{
    Body: PromptRequest;
  }>(
    "/prompt",
    {
      schema: {
        summary: "Submit Prompt",
        description: "Submit an API-formatted ComfyUI prompt.",
        body: PromptRequestSchema,
        response: {
          200: PromptResponseSchema,
          202: PromptResponseSchema,
          400: PromptErrorResponseSchema,
        },
      },
    },
    async (request, reply) => {
      let { prompt, id, webhook } = request.body;
      let batchSize = 1;
      let inputLocalFilePath = "";

      for (const nodeId in prompt) {
        const node = prompt[nodeId];
        if (node.class_type === "SaveImage" || node.class_type === "SaveImageExtended") {
          node.inputs.filename_prefix = id;
        }
        else if (node.inputs.batch_size) {
          batchSize = node.inputs.batch_size;
        } else if (node.class_type === "LoadImage") {
          const imageInput = node.inputs.image;
          try {
            node.inputs.image = await processImage(imageInput, app.log);
            inputLocalFilePath = path.join(config.inputDir, node.inputs.image);
          } catch (e: any) {
            if (inputLocalFilePath) {
              fsPromises.unlink(inputLocalFilePath);
            }
            return reply.code(400).send({
              error: e.message,
              location: `prompt.${nodeId}.inputs.image`,
            });
          }
        }
      }

      if (webhook) {
        outputWatcher.addPrefixAction(
          id,
          batchSize,
          async (filepath: string) => {
            const base64File = await fsPromises.readFile(filepath, {
              encoding: "base64",
            });
            try {
              const res = await fetch(webhook, {
                method: "POST",
                headers: createHeaders(request.headers),
                body: JSON.stringify({
                  image: base64File,
                  id,
                  filename: path.basename(filepath),
                  prompt,
                }),
              });
              if (!res.ok) {
                app.log.error(
                  `Failed to send image to webhook: ${await res.text()}`
                );
              }
            } catch (e: any) {
              app.log.error(`Failed to send image to webhook: ${e.message}`);
            }

            // Remove the ouput and input files after sending
            fsPromises.unlink(filepath);
            if (inputLocalFilePath) {
              fsPromises.unlink(inputLocalFilePath);
            }
          }
        );

        const resp = await fetch(`${config.comfyURL}/prompt`, {
          method: "POST",
          headers: createHeaders(request.headers),
          body: JSON.stringify({ prompt }),
        });

        if (!resp.ok) {
          outputWatcher.removePrefixAction(id);
          return reply.code(resp.status).send({ error: await resp.text() });
        }
        return reply.code(202).send({ status: "ok", id, webhook, prompt });
      } else {
        // Wait for the file and return it
        const images: string[] = [];
        function waitForImagesToGenerate(): Promise<void> {
          return new Promise((resolve, reject) => {
            const timeout = setTimeout(() => {
              outputWatcher.removePrefixAction(id);
              reject(new Error('Image generation timeout'));
            }, 300000); // 5 minute timeout

            outputWatcher.addPrefixAction(
              id,
              batchSize,
              async (filepath: string) => {
                try {
                  server.log.info(`dai dai generated file: ${filepath}`);
                  const base64File = await fsPromises.readFile(filepath, {
                    encoding: "base64",
                  });
                  server.log.info(`dai dai file size: ${base64File.length}`);
                  images.push(base64File);

                  // Remove the input and output files after reading
                  fsPromises.unlink(filepath);
                  if (inputLocalFilePath) {
                    fsPromises.unlink(inputLocalFilePath);
                  }

                  if (images.length === batchSize) {
                    clearTimeout(timeout);
                    outputWatcher.removePrefixAction(id);
                    resolve();
                  }
                } catch (err) {
                  clearTimeout(timeout);
                  outputWatcher.removePrefixAction(id);
                  reject(err);
                }
              }
            );
          });
        }

        const finished = waitForImagesToGenerate();
        const resp = await fetch(`${config.comfyURL}/prompt`, {
          method: "POST",
          headers: createHeaders(request.headers),
          body: JSON.stringify({ prompt }),
        });
        if (!resp.ok) {
          outputWatcher.removePrefixAction(id);
          return reply.code(resp.status).send({ error: await resp.text() });
        }
        await finished;

        const respJson = { id, images }; // we do not need prompt, it take lot of space if the input and prompt has image data as base64 in it
        return reply.send(respJson);
      }
    }
  );


  // Recursively build the route tree from workflows
  const walk = (tree: WorkflowTree, route = "/workflow") => {
    for (const key in tree) {
      const node = tree[key];
      if (isWorkflow(node)) {
        const BodySchema = z.object({
          id: z
            .string()
            .optional()
            .default(() => randomUUID()),
          input: node.RequestSchema,
          webhook: z.string().optional(),
        });

        type BodyType = z.infer<typeof BodySchema>;

        let description = "";
        if (config.markdownSchemaDescriptions) {
          description = zodToMarkdownTable(node.RequestSchema);
        } else if (node.description) {
          description = node.description;
        }

        let summary = key;
        if (node.summary) {
          summary = node.summary;
        }

        app.post<{
          Body: BodyType;
        }>(
          `${route}/${key}`,
          {
            schema: {
              summary,
              description,
              body: BodySchema,
              response: {
                200: WorkflowResponseSchema,
                202: WorkflowResponseSchema,
              },
            },
          },
          async (request, reply) => {
            const { id, input, webhook } = request.body;
            const prompt = node.generateWorkflow(input);

            const resp = await fetch(
              `http://localhost:${config.wrapperPort}/prompt`,
              {
                method: "POST",
                headers: createHeaders(request.headers),
                body: JSON.stringify({ prompt, id, webhook }),
              }
            );
            const body = await resp.json();
            if (!resp.ok) {
              return reply.code(resp.status).send(body);
            }

            // we do not need these info, it take lot of space if the input and prompt has image data as base64 in it
            body.input = {};
            // body.prompt = {};

            return reply.code(resp.status).send(body);
          }
        );

        server.log.info(`Registered workflow ${route}/${key}`);

        // store all of the workflow routes
        allWorkflows.push(`${route}/${key}`);
        server.log.info(`dai dai after allWorkflows: ${allWorkflows}`);
      } else {
        walk(node as WorkflowTree, `${route}/${key}`);
      }
    }
  };
  walk(workflows);

  app.post<{
    Body: QueueRequest;
  }>(
    "/queue",
    {
      schema: {
        summary: "Queue Workflow Task",
        description: "Submit a workflow task to be processed asynchronously",
        body: QueueRequestSchema,
        response: {
          200: WorkflowResponseSchema,
          202: WorkflowResponseSchema,
          400: PromptErrorResponseSchema,
        },
      },
    },
    async (request, reply) => {
      const { workflowRoute, workflowInput, api_version } = request.body;

      // Validate api_version
      if (isNaN(Number(api_version))) {
        return reply.code(444).send({
          error: `Invalid API version: ${api_version}. Must be a number.`,
          location: "body.api_version",
        });
      }

      // check if the workflowRoute is valid
      server.log.info(`dai dai request api_version: ${api_version}`);
      if (api_version != config.currentApiVersion) {
        server.log.info(`dai dai wrong api version: ${api_version}`);
        return reply.code(444).send({
          error: `API version mismatch. Expected ${config.currentApiVersion}, got ${api_version}`,
          location: "body.api_version",
        });
      }

      server.log.info(`dai dai current allWorkflows: ${allWorkflows}`);
      server.log.info(`dai dai current workflowRoute: ${workflowRoute}`);
      if (!allWorkflows.includes(workflowRoute)) {
        return reply.code(400).send({
          error: `Invalid workflow route: ${workflowRoute}`,
          location: "workflowRoute in the post body",
        });
      }

      // if valid workflow route, call the workflow api
      const workflowResp = await fetch(
        `http://localhost:${config.wrapperPort}${workflowRoute}`,
        {
          method: "POST",
          headers: createHeaders(request.headers),
          body: JSON.stringify(workflowInput),
        }
      );

      const body = await workflowResp.json();
      if (!workflowResp.ok) {
        return reply.code(workflowResp.status).send(body);
      }

      return reply.code(workflowResp.status).send(body);
    }
  );
});

let warm = false;

process.on("SIGINT", async () => {
  server.log.info("Received SIGINT, interrupting process");
  shutdownComfyUI();
  await outputWatcher.stopWatching();
  process.exit(0);
});

export async function start() {
  try {
    const start = Date.now();
    // Start the command
    launchComfyUI();
    await waitForComfyUIToStart(server.log);

    await server.ready();
    server.swagger();

    // Start the server
    await server.listen({ port: config.wrapperPort, host: config.wrapperHost });
    server.log.info(`ComfyUI API ${version} started.`);
    await warmupComfyUI();
    warm = true;
    const warmupTime = Date.now() - start;
    server.log.info(`Warmup took ${warmupTime / 1000}s`);
  } catch (err: any) {
    server.log.error(`Failed to start server: ${err.message}`);
    process.exit(1);
  }
}

function createHeaders(oldHeaders: Record<string, any>): Record<string, string> {
  return {
    "Content-Type": "application/json",
  };
}