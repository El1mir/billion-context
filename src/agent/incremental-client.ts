// Shared incremental-turn client for native extensions (#3248).
//
// Lives in the HOST process (bundled with each native adapter: pi / omp /
// fetch-patched native lanes) and owns the per-conversation ack state:
//   1. probe the lane once per proxy base (GET /__bili/plugin/turn, TTL
//      cache; 404/failed → stay full-resend, byte-identical to today);
//   2. diff the outgoing wire messages against the last acknowledged set —
//      matching prefix → send only the delta; mismatch (edit / branch) →
//      resync with the full history; the server's ack header is the only
//      authority for sinceSeq;
//   3. any lane-level failure (409 storm, 503) falls back to the original
//      full-resend request with the marker removed — behavior equals the
//      pre-incremental world for that turn.
//
// Wire contract: see PLUGIN.md "Incremental turns". Messages are raw wire
// messages ({role, content: string | blocks[]}) — the lane derives stable
// content ids server-side, so hosts without their own message ids work.

import { createHash } from "node:crypto";

/** Marker header the lane's divert recognizes. Client-side copy of
 * INCREMENTAL_TURN_MARKER_HEADER in src/incremental-lane.ts — the host-side
 * helper must not import server modules. Keep the two in sync. */
export const INCREMENTAL_TURN_MARKER_HEADER = "x-bili-plugin-turn";

/** Same allowlist the lane accepts in `request` (mirror of
 * REQUEST_PARAM_KEYS in src/incremental-lane.ts — keep in sync). */
const REQUEST_PARAM_KEYS = [
    "model", "max_tokens", "temperature", "top_p", "top_k", "stop_sequences",
    "stream", "tools", "tool_choice", "metadata", "thinking", "service_tier",
    "parallel_tool_calls",
] as const;

export interface IncrementalWireMessage {
    role: string;
    content: unknown;
}

interface ConvState {
    /** Last server ack (x-bili-applied-seq) — flat message count. */
    sinceSeq: number;
    /** Wire hashes of everything the server has acknowledged, in order. */
    sent: string[];
    /** Sticky system (JSON) we told the lane; resend only on change. */
    systemJson: string | undefined;
    /** Per-conversation kill switch after unrecoverable failures. */
    dead: boolean;
}

const convs = new Map<string, ConvState>();

const PROBE_TTL_MS = 30_000;
const probeCache = new Map<string, { ok: boolean; at: number }>();

/** Probe the lane's incremental support. Cached per proxy base for 30s; any
 * error or non-200 → not enabled (older servers 404 the GET). */
export async function probeIncrementalTurns(proxyBase: string, fetchImpl: typeof fetch = fetch): Promise<boolean> {
    const now = Date.now();
    const cached = probeCache.get(proxyBase);
    if (cached && now - cached.at < PROBE_TTL_MS) return cached.ok;
    let ok = false;
    try {
        const res = await fetchImpl(`${proxyBase}/__bili/plugin/turn`, { method: "GET", signal: AbortSignal.timeout(3000) });
        if (res.ok) {
            const body = await res.json() as { enabled?: unknown };
            ok = body.enabled === true;
        }
    } catch {
        ok = false;
    }
    probeCache.set(proxyBase, { ok, at: now });
    return ok;
}

/** Does this message list look like the Anthropic wire shape the lane's
 *  anthropicToCore can ingest? OpenAI-tool shapes (role:"tool"/"function",
 *  assistant.tool_calls, tool_call_id) are NOT convertible — the adapter
 *  falls back to a full resend for those conversations (correctness over
 *  savings). Content may be a string or a block array, both fine. */
export function wireMessagesLookAnthropic(messages: unknown): messages is IncrementalWireMessage[] {
    if (!Array.isArray(messages) || messages.length === 0) return false;
    for (const m of messages) {
        if (m === null || typeof m !== "object") return false;
        const msg = m as { role?: unknown; content?: unknown; tool_calls?: unknown; tool_call_id?: unknown };
        if (typeof msg.role !== "string" || msg.role === "tool" || msg.role === "function") return false;
        if (msg.tool_calls !== undefined || msg.tool_call_id !== undefined) return false;
        if (typeof msg.content !== "string" && !Array.isArray(msg.content)) return false;
    }
    return true;
}

function wireHash(m: IncrementalWireMessage): string {
    return createHash("sha256").update(`${m.role}\u0000${JSON.stringify(m.content)}`).digest("hex").slice(0, 16);
}

type IncrementalPlan =
    | { mode: "seed"; body: string }            // full history, sinceSeq 0 (server may 409 → resync)
    | { mode: "delta"; body: string }           // suffix only, acked sinceSeq
    | { mode: "resync"; body: string }          // full history, resync: true (recovery)
    | { mode: "skip" };                         // nothing to gain / state dead → full resend

interface PlanArgs {
    conversationId: string;
    /** Forward path of the ORIGINAL request (the marker divert uses it);
     * optional — the lane defaults a bare `path` to /v1/messages. */
    path?: string;
    messages: IncrementalWireMessage[];
    /** Raw wire system field; omit key entirely when the host sent none. */
    system: unknown;
    /** Anthropic request params from the original body (model, max_tokens…);
     * optional — missing params ride the lane's defaults. */
    request?: Record<string, unknown>;
}

function buildBody(args: PlanArgs, sinceSeq: number, messages: IncrementalWireMessage[], resync: boolean, includeSystem: boolean): string {
    // `request` is optional at the call sites (a host may plan with only
    // messages+system); missing params simply ride their lane defaults.
    const src = args.request ?? {};
    const request: Record<string, unknown> = {};
    for (const key of REQUEST_PARAM_KEYS) if (key in src) request[key] = src[key];
    const body: Record<string, unknown> = {
        conversationId: args.conversationId,
        sinceSeq,
        path: args.path ?? "/v1/messages",
        request,
        messages,
    };
    if (resync) body.resync = true;
    if (includeSystem) body.system = args.system;
    return JSON.stringify(body);
}

/** Plan the incremental form of one outgoing model request. `forceResync`
 * replans after a 409 (seed hit a live canonical, or an edit diverged). */
export function planIncrementalTurn(args: PlanArgs, forceResync = false): IncrementalPlan {
    const state = convs.get(args.conversationId);
    if ((state !== undefined && state.dead) || args.messages.length === 0) return { mode: "skip" };
    if (state === undefined || forceResync) {
        const systemJson = args.system === undefined ? undefined : JSON.stringify(args.system);
        const includeSystem = state === undefined || systemJson !== state.systemJson;
        // First attempt (and every forced replan) sends the FULL set with
        // resync:true — a wholesale canonical replacement never 409s, so the
        // first incremental turn works whether the server-side history is
        // empty (fresh conversation) or already live (interop with
        // full-resend traffic: the client's view is authoritative and
        // identical-or-newer). Only DELTA turns can 409.
        return { mode: forceResync ? "resync" : "seed", body: buildBody(args, 0, args.messages, true, includeSystem) };
    }
    const hashes = args.messages.map(wireHash);
    // Prefix alignment: everything previously acked must still lead the list.
    const n = Math.min(state.sent.length, hashes.length);
    let aligned = true;
    for (let i = 0; i < n; i++) {
        if (state.sent[i] !== hashes[i]) { aligned = false; break; }
    }
    if (!aligned || hashes.length < state.sent.length) {
        return { mode: "resync", body: buildBody(args, 0, args.messages, true, args.system === undefined ? false : JSON.stringify(args.system) !== state.systemJson) };
    }
    if (hashes.length === state.sent.length) {
        // Equal message set: normally a retry of the same turn → normal path.
        // But a SYSTEM change is still a real outgoing turn — send an empty
        // delta so the lane forwards the assembled request with the new
        // sticky system (the canonical itself is unchanged).
        const sysJson = args.system === undefined ? undefined : JSON.stringify(args.system);
        if (sysJson !== undefined && sysJson !== state.systemJson) {
            return { mode: "delta", body: buildBody(args, state.sinceSeq, [], false, true) };
        }
        return { mode: "skip" }; // retry of the same turn → normal path
    }
    const systemJson = args.system === undefined ? undefined : JSON.stringify(args.system);
    return {
        mode: "delta",
        body: buildBody(args, state.sinceSeq, args.messages.slice(state.sent.length), false, systemJson !== undefined && systemJson !== state.systemJson),
    };
}

/** Record a 200 ack: `appliedSeq` is the response's x-bili-applied-seq and
 * `messages` the wire set the server just acknowledged. */
export function noteIncrementalAck(conversationId: string, appliedSeq: number, messages: IncrementalWireMessage[], system: unknown): void {
    convs.set(conversationId, {
        sinceSeq: appliedSeq,
        sent: messages.map(wireHash),
        systemJson: system === undefined ? undefined : JSON.stringify(system),
        dead: false,
    });
}

/** Sticky per-conversation fallback after an unrecoverable lane failure —
 * the conversation keeps working via plain full resend. */
export function markIncrementalDead(conversationId: string): void {
    const state = convs.get(conversationId);
    if (state) state.dead = true;
}

/** Drop the cached probe for one proxy base (e.g. after a 503 — the lane's
 * rollout flag or sidecar liveness changed). The next turn re-probes. */
export function clearIncrementalProbe(proxyBase: string): void {
    probeCache.delete(proxyBase);
}

export function __resetIncrementalClientForTests(): void {
    convs.clear();
    probeCache.clear();
}
