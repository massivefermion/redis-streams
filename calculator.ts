import Redis from "ioredis";

const listener = new Redis();
const publisher = new Redis();

function format([_id, fields]) {
  const entries: any[] = [];
  for (let n = 0; n < fields.length - 1; n = n + 2) {
    entries.push([fields[n], fields[n + 1]]);
  }
  return { _id, ...Object.fromEntries(entries) };
}

async function* listen(last_id = "$") {
  const results = await listener.xread("BLOCK", 0, "STREAMS", "jobs", last_id);

  if (results) {
    const [_key, messages] = results[0];

    const formatted = messages.map(format);
    for (const msg of formatted) {
      yield msg;
    }

    yield* listen(messages[messages.length - 1][0]);
  }
}

const operations = Object.freeze({
  "*": (a, b) => a * b,
  "/": (a, b) => a / b,
  "+": (a, b) => a + b,
  "-": (a, b) => a - b,
});

async function main() {
  for await (const msg of listen()) {
    const result = operations[msg.op](parseInt(msg.left), parseInt(msg.right));
    await publisher.xadd(
      "jobs-response",
      "*",
      "jobId",
      msg.jobId,
      "result",
      result
    );
  }
}

main().then(process.exit.bind(null, 0));
