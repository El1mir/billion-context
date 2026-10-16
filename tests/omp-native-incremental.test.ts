// #3248 phase 2 — omp native incremental adapter. omp never emits header
// events, so the ONLY identity signal at the fetch layer is prompt_cache_key
// (stamped into the body by the omp extension). These suites drive the two
// exported seams (ompIncrementalBeforeSend / ompIncrementalOnResponse) with a
// fake lane origin pinned via _setOmpOriginForTest.
import { after, test } from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";

process.env.NODE_TEST_CONTEXT = "1";

const { ompIncrementalBeforeSend, ompIncrementalOnResponse, _setOmpOriginForTest, __resetOmpIncrementalForTests } = await import("../src/agent/omp-native.ts");
const { __resetIncrementalClientForTests } = await import("../src/agent/incremental-client.ts");

const MODEL_URL = "https://api.openai-anthropic.example/v1/messages";

async function fakeLane(enabled: boolean): Promise<{ base: string; close: () => Promise<void> }> {
    const srv = createServer((req, res) => {
        if (req.method === "GET" && req.url === "/__bili/plugin/turn") {
            res.writeHead(200, { "content-type": "application/json" });
            res.end(JSON.stringify({ ok: true, protocol: 1, feature: "pluginIncrementalTurns", enabled, storage: enabled ? "unified" : "legacy-only" }));
            return;
        }
        res.writeHead(404); res.end("{}");
    });
    await new Promise<void>((r) => srv.listen(0, "127.0.0.1", r));
    const { port } = srv.address() as AddressInfo;
    return { base: `http://127.0.0.1:${port}`, close: () => new Promise<void>((r) => srv.close(() => r())) };
}

function initOf(body: unknown): RequestInit {
    return { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) };
}

const lanes: { close: () => Promise<void> }[] = [];
after(async () => {
    for (const l of lanes) await l.close();
    _setOmpOriginForTest(undefined);
    __resetIncrementalClientForTests();
    __resetOmpIncrementalForTests();
});

test("omp incremental: prompt_cache_key becomes the conversation id; plugin headers + marker land on init; delta after ack", async () => {
    __resetIncrementalClientForTests();
    __resetOmpIncrementalForTests();
    const lane = await fakeLane(true);
    lanes.push(lane);
    _setOmpOriginForTest(lane.base);

    const msgs1 = [{ role: "user", content: "hello omp" }];
    const init = initOf({ model: "claude-test", max_tokens: 512, prompt_cache_key: "omp-inc-1", messages: msgs1 });
    await ompIncrementalBeforeSend(MODEL_URL, init);
    const body = JSON.parse(String(init.body)) as Record<string, unknown>;
    assert.equal(body.conversationId, "omp-inc-1", "prompt_cache_key rides as the conversation id");
    assert.equal(body.sinceSeq, 0);
    assert.equal((body.messages as unknown[]).length, 1);
    const headers = init.headers as Headers;
    assert.equal(headers.get("x-bili-plugin"), "omp");
    assert.equal(headers.get("x-bili-plugin-conversation"), "omp-inc-1");
    assert.equal(headers.get("x-bili-plugin-turn"), "1");

    ompIncrementalOnResponse(MODEL_URL, new Response("{}", { status: 200, headers: { "x-bili-applied-seq": "1" } }));

    const msgs2 = [...msgs1, { role: "assistant", content: "hi" }, { role: "user", content: "next" }];
    const init2 = initOf({ model: "claude-test", max_tokens: 512, prompt_cache_key: "omp-inc-1", messages: msgs2 });
    await ompIncrementalBeforeSend(MODEL_URL, init2);
    const body2 = JSON.parse(String(init2.body)) as Record<string, unknown>;
    assert.equal(body2.sinceSeq, 1, "acked watermark rides");
    assert.equal((body2.messages as unknown[]).length, 2, "delta only");
});

test("omp incremental: no prompt_cache_key (plugin inactive) leaves the init untouched", async () => {
    __resetIncrementalClientForTests();
    __resetOmpIncrementalForTests();
    const lane = await fakeLane(true);
    lanes.push(lane);
    _setOmpOriginForTest(lane.base);
    const original = { model: "claude-test", max_tokens: 512, messages: [{ role: "user", content: "hi" }] };
    const init = initOf(original);
    await ompIncrementalBeforeSend(MODEL_URL, init);
    assert.deepEqual(JSON.parse(String(init.body)), original, "unstamped body = extension inactive = full resend");
    assert.deepEqual(init.headers, { "content-type": "application/json" }, "headers untouched when no plan lands");
});

test("omp incremental: probe disabled leaves the body untouched", async () => {
    __resetIncrementalClientForTests();
    __resetOmpIncrementalForTests();
    const lane = await fakeLane(false);
    lanes.push(lane);
    _setOmpOriginForTest(lane.base);
    const original = { model: "claude-test", max_tokens: 512, prompt_cache_key: "omp-inc-off", messages: [{ role: "user", content: "hi" }] };
    const init = initOf(original);
    await ompIncrementalBeforeSend(MODEL_URL, init);
    assert.deepEqual(JSON.parse(String(init.body)), original);
});
