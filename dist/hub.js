"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.receive = exports.publish = exports.sendOp = exports.listen = exports.init = exports.subscriber = void 0;
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
const sender = new ioredis_1.Cluster(nodes);
const publisher = new ioredis_1.Cluster(nodes);
exports.subscriber = new ioredis_1.Cluster(nodes);
function format_message(key, [_id, fields]) {
    const entries = [];
    for (let n = 0; n < fields.length - 1; n = n + 2) {
        entries.push([fields[n], fields[n + 1]]);
    }
    return { _id, _from: key, ...Object.fromEntries(entries) };
}
async function init(keys) {
    for await (const key of keys)
        try {
            await listener.xgroup("CREATE", key, "cg", "$", "MKSTREAM");
        }
        catch { }
    await exports.subscriber.subscribe("misfits");
}
exports.init = init;
async function* listen(instance, streams) {
    const results = await listener.xreadgroup("GROUP", "cg", instance, "BLOCK", 0, "NOACK", "STREAMS", ...streams.map(({ key, last_id: _ }) => key), ...streams.map(({ key: _, last_id: __ }) => ">"));
    if (results) {
        let new_streams = [];
        for (const [key, messages] of results) {
            const formatted = messages.map(format_message.bind(null, key));
            for (const msg of formatted) {
                yield msg;
            }
            new_streams.push({ key, last_id: messages[messages.length - 1][0] });
        }
        const missed = streams.filter((s) => !new_streams.some(({ key, last_id: _ }) => key == s.key));
        new_streams = new_streams.concat(missed);
        yield* listen(instance, new_streams);
    }
}
exports.listen = listen;
async function sendOp(jobId, op, left, right) {
    await sender.xadd("jobs", "*", "jobId", jobId, "op", op, "left", left, "right", right);
}
exports.sendOp = sendOp;
async function publish(msg) {
    await publisher.publish("misfits", JSON.stringify(msg));
}
exports.publish = publish;
function receive(subscriber) {
    return new Promise((resolve) => {
        subscriber.on("message", (...args) => resolve(args));
    });
}
exports.receive = receive;
