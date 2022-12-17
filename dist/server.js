"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const fastify_1 = __importDefault(require("fastify"));
const uuid_1 = require("uuid");
const hub_1 = require("./hub");
const fastify = (0, fastify_1.default)({ logger: false });
const msgSrc = (0, hub_1.listen)(process.env.NODE_APP_INSTANCE || "single", [
    { key: "jobs-response" },
]);
async function listenForId(jobId) {
    let { value: msg } = await msgSrc.next();
    if (msg.jobId == jobId)
        return msg;
    await (0, hub_1.publish)(msg);
    msg = await (0, hub_1.receive)(hub_1.subscriber);
    if (msg.jobId == jobId)
        return msg;
    return null;
}
const ops = ["*", "/", "+", "-"];
fastify.get("/", async () => {
    // const { op, left, right } = request.body;
    const op = ops[Math.floor(Math.random() * ops.length)];
    const left = Math.random() * 1024;
    const right = Math.random() * 1024;
    const jobId = (0, uuid_1.v4)();
    await (0, hub_1.sendOp)(jobId, op, left, right);
    const { result } = await listenForId(jobId);
    return { result };
});
(0, hub_1.init)(["jobs-response"]).then((err) => {
    fastify.listen({ port: 3000 }, () => { });
});
