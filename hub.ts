import Redis from "ioredis";

const listener = new Redis();
const publisher = new Redis();

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

export async function publish(jobId, op, left, right) {
  await publisher.xadd(
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
