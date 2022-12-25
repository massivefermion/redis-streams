import { randomUUID } from "crypto";
import Fastify from "fastify";
import { init, listen, sendOp } from "./hub";

const fastify = Fastify({ logger: false });

const msgSrc = listen((process.env.NODE_APP_INSTANCE as string) || "single", [
  { key: "jobs-response" },
]);

async function listenForId(jobId: string) {
  let { value: msg } = await msgSrc.next();
  if (msg.jobId == jobId) return msg;
  return listenForId(jobId);
  // await publish(msg);
  // msg = await receive(subscriber);
  // if (msg.jobId == jobId) return msg;
  // return null;
}

const ops = ["*", "/", "+", "-"];
fastify.get("/", async () => {
  // const { op, left, right } = request.body;
  const op = ops[Math.floor(Math.random() * ops.length)];
  const left = Math.random() * 1024;
  const right = Math.random() * 1024;
  const jobId = randomUUID();
  await sendOp(jobId, op, left, right);
  const { result } = await listenForId(jobId);
  return { result };
});

init(["jobs-response"]).then(() => {
  fastify.listen({ port: 3000 }, () => {});
});
