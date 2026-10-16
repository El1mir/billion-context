// #3248 phase 2 — opencode incremental adapter. Drives the two exported
// seams directly (maybeIncrementalOpencode / opencodeIncrementalOnResponse)
// against a fake lane origin: the probe endpoint decides enablement, the
// body/marker assertions pin the divert contract, and the ack→delta pair
// pins the state machine ride-along. The full e2e (marker divert through
// the real server) lives in incremental-lane.test.ts.
import { after, test } from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";

process.env.NODE_TEST_CONTEXT = "1";

const { maybeIncrementalOpencode, opencodeIncrementalOnResponse, __resetOpencodeIncrementalForTests } = await import("../src/agent/opencode-native.ts");
const { __resetIncrementalClientForTests } = await import("../src/agent/incremental-client.ts");
import type { V2HttpRequestEvent, V2State } from "../src/agent/opencode-v2.ts";

const MODEL_URL = "https://api.anthropic.com/v1/messages";

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

function fakeEvent(body: unknown, sid: string): { e: V2HttpRequestEvent; url: string } {
    const req = new Request(MODEL_URL, {
        method: "POST",
        headers: { "content-type": "application/json", authorization: "Bearer sk-test" },
        body: JSON.stringify(body),
    });
    const e = { request: req, sessionID: sid } as unknown as V2HttpRequestEvent;
    return { e, url: req.url };
}

function state(base: string): V2State {
    return { proxyBase: base } as unknown as V2State;
}

const lanes: { close: () => Promise<void> }[] = [];
after(async () => {
    for (const l of lanes) await l.close();
    __resetIncrementalClientForTests();
    __resetOpencodeIncrementalForTests();
});

test("opencode incremental: seed swap (marker + shaped body, headers preserved), ack, then delta", async () => {
    __resetIncrementalClientForTests();
    __resetOpencodeIncrementalForTests();
    const lane = await fakeLane(true);
    lanes.push(lane);
    const s = state(lane.base);

    const msgs1 = [{ role: "user", content: "hello opencode" }];
    const { e, url } = fakeEvent({ model: "claude-test", max_tokens: 512, system: "be terse", messages: msgs1 }, "oc-inc-1");
    await maybeIncrementalOpencode(e, s);
    const swapped = e.request as Request;
    assert.notEqual(swapped.url, undefined);
    const body = JSON.parse(await swapped.clone().json().then((j) => JSON.stringify(j))) as Record<string, unknown>;
    assert.equal(body.conversationId, "oc-inc-1");
    assert.equal(body.sinceSeq, 0);
    assert.equal((body.messages as unknown[]).length, 1);
    assert.equal(body.model, undefined, "incremental payload carries model inside request, not top-level");
    assert.equal(swapped.headers.get("x-bili-plugin-turn"), "1", "marker stamped on the replaced request");
    assert.equal(swapped.headers.get("authorization"), "Bearer sk-test", "original headers survive the swap");

    // ack advances the helper state machine
    opencodeIncrementalOnResponse(url, new Response("{}", { status: 200, headers: { "x-bili-applied-seq": "1" } }));

    const msgs2 = [...msgs1, { role: "assistant", content: "hi" }, { role: "user", content: "next" }];
    const t2 = fakeEvent({ model: "claude-test", max_tokens: 512, system: "be terse", messages: msgs2 }, "oc-inc-1");
    await maybeIncrementalOpencode(t2.e, s);
    const body2 = JSON.parse(await (t2.e.request as Request).clone().json().then((j) => JSON.stringify(j))) as Record<string, unknown>;
    assert.equal(body2.sinceSeq, 1, "second turn rides the acked watermark");
    assert.equal((body2.messages as unknown[]).length, 2, "only the delta rides");
});

test("opencode incremental: probe disabled leaves the request untouched", async () => {
    __resetIncrementalClientForTests();
    __resetOpencodeIncrementalForTests();
    const lane = await fakeLane(false);
    lanes.push(lane);
    const original = { model: "claude-test", max_tokens: 512, messages: [{ role: "user", content: "hello" }] };
    const { e } = fakeEvent(original, "oc-inc-off");
    await maybeIncrementalOpencode(e, state(lane.base));
    const body = await (e.request as Request).clone().json();
    assert.deepEqual(body, original, "request body is byte-equivalent when the feature is off");
    assert.equal((e.request as Request).headers.get("x-bili-plugin-turn"), null);
});

test("opencode incremental: OpenAI-tool shapes never swap", async () => {
    __resetIncrementalClientForTests();
    __resetOpencodeIncrementalForTests();
    const lane = await fakeLane(true);
    lanes.push(lane);
    const original = { model: "gpt-test", max_tokens: 512, messages: [{ role: "assistant", content: "x", tool_calls: [{ id: "c", type: "function", function: { name: "f" } }] }] };
    const { e } = fakeEvent(original, "oc-inc-tool");
    await maybeIncrementalOpencode(e, state(lane.base));
    const body = await (e.request as Request).clone().json();
    assert.deepEqual(body, original, "non-anthropic wire shapes ride full resend");
});

test("opencode incremental: 409 on a delta marks the conversation dead — next turn falls back", async () => {
    __resetIncrementalClientForTests();
    __resetOpencodeIncrementalForTests();
    const lane = await fakeLane(true);
    lanes.push(lane);
    const s = state(lane.base);
    // seed → ack → delta rejected 409 → dead: the host's own full resend takes over
    const msgs1 = [{ role: "user", content: "hello" }];
    const t1 = fakeEvent({ model: "claude-test", max_tokens: 512, messages: msgs1 }, "oc-inc-dead");
    await maybeIncrementalOpencode(t1.e, s);
    opencodeIncrementalOnResponse(t1.url, new Response("{}", { status: 200, headers: { "x-bili-applied-seq": "1" } }));
    const msgs2 = [...msgs1, { role: "assistant", content: "hi" }, { role: "user", content: "again" }];
    const t2 = fakeEvent({ model: "claude-test", max_tokens: 512, messages: msgs2 }, "oc-inc-dead");
    await maybeIncrementalOpencode(t2.e, s);
    const t2Body = JSON.parse(await (t2.e.request as Request).clone().json().then((j) => JSON.stringify(j))) as { sinceSeq: number };
    assert.equal(t2Body.sinceSeq, 1, "delta planned");
    // the delta was parsed from the CLONED request — ack with the ORIGINAL t2 url
    opencodeIncrementalOnResponse(t2.url, new Response("{}", { status: 409, headers: { "x-bili-server-seq": "1" } }));
    const original = { model: "claude-test", max_tokens: 512, messages: [...msgs2, { role: "user", content: "more" }] };
    const t3 = fakeEvent(original, "oc-inc-dead");
    await maybeIncrementalOpencode(t3.e, s);
    const body = await (t3.e.request as Request).clone().json();
    assert.deepEqual(body, original, "dead conversation resends full via the host path");
});
