"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const ioredis_1 = require("ioredis");
const dotenv_1 = __importDefault(require("dotenv"));
dotenv_1.default.config();
if (!process.env.nodes)
    throw new Error();
const nodes = process.env.nodes.split(",").map((node) => {
    const [host, port] = node.split(":");
    return { host, port: parseInt(port) || 6379 };
});
const listener = new ioredis_1.Cluster(nodes);
const publisher = new ioredis_1.Cluster(nodes);
function format([_id, fields]) {
    const entries = [];
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
        await publisher.xadd("jobs-response", "*", "jobId", msg.jobId, "result", result);
    }
}
main().then(process.exit.bind(null, 0));
