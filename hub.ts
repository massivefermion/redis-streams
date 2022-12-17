import { Cluster } from "ioredis";
import dotenv from "dotenv";

dotenv.config();

if (!process.env.nodes) throw new Error();
const nodes = process.env.nodes.split(",").map((node) => {
  const [host, port] = node.split(":");
  return { host, port: parseInt(port) || 6379 };
});

const listener = new Cluster(nodes);
const sender = new Cluster(nodes);

const publisher = new Cluster(nodes);
export const subscriber = new Cluster(nodes);

function format_message(key, [_id, fields]) {
  const entries: any[] = [];
  for (let n = 0; n < fields.length - 1; n = n + 2) {
    entries.push([fields[n], fields[n + 1]]);
  }
  return { _id, _from: key, ...Object.fromEntries(entries) };
}

export async function init(keys: string[]) {
  for await (const key of keys)
    try {
      await listener.xgroup("CREATE", key, "cg", "$", "MKSTREAM");
    } catch {}

  await subscriber.subscribe("misfits");
}

export async function* listen(
  instance: string,
  streams: { key: string; last_id?: string }[]
) {
  const results: any[] = await listener.xreadgroup(
    "GROUP",
    "cg",
    instance,
    "BLOCK",
    0,
    "NOACK",
    "STREAMS",
    ...streams.map(({ key, last_id: _ }) => key),
    ...streams.map(({ key: _, last_id: __ }) => ">")
  );

  if (results) {
    let new_streams: any[] = [];
    for (const [key, messages] of results) {
      const formatted = messages.map(format_message.bind(null, key));
      for (const msg of formatted) {
        yield msg;
      }

      new_streams.push({ key, last_id: messages[messages.length - 1][0] });
    }

    const missed = streams.filter(
      (s) => !new_streams.some(({ key, last_id: _ }) => key == s.key)
    );
    new_streams = new_streams.concat(missed);

    yield* listen(instance, new_streams);
  }
}

export async function sendOp(jobId, op, left, right) {
  await sender.xadd(
    "jobs",
    "*",
    "jobId",
    jobId,
    "op",
    op,
    "left",
    left,
    "right",
    right
  );
}

export async function publish(msg) {
  await publisher.publish("misfits", JSON.stringify(msg));
}

export function receive(subscriber) {
  return new Promise((resolve) => {
    subscriber.on("message", (...args) => resolve(args));
  });
}
