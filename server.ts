import Fastify from "fastify";
import { v4 as uuid } from "uuid";
import { init, listen, publish } from "./hub";

const fastify = Fastify({ logger: true });

async function listenForId(jobId: string) {
  const msgSrc = listen((process.env.NODE_APP_INSTANCE as string) || "single", [
    { key: "jobs-response" },
  ]);

  const { value: msg } = await msgSrc.next();
  if (msg.jobId == jobId) return msg;
  return listenForId(jobId);
}

const ops = ["*", "/", "+", "-"];
fastify.get("/", async () => {
  // const { op, left, right } = request.body;
  const op = ops[Math.floor(Math.random() * ops.length)];
  const left = Math.random() * 1024;
  const right = Math.random() * 1024;
  const jobId = uuid();
  await publish(jobId, op, left, right);
  const { result } = await listenForId(jobId);
  return { result };
});

init(["jobs-response"]).then(() => {
  fastify.listen({ port: 3000 }, () => {});
});
