import { test } from "node:test";
import assert from "node:assert/strict";
import { tmpdir } from "node:os";
import { mkdtempSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { SessionStore } from "../src/persist.ts";
import type { Session } from "../src/session.ts";
import { createInitialState } from "acp-kernel";
import { rmrf } from "./tmp-rm.ts";

function sleepSync(ms: number): void {
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

// #3049: the stale-disk guard's ownership fast path. The guard protects the
// shared sessions dir from counter rollback (#405/#2401); it used to re-read
// and re-parse the ENTIRE envelope on every save for two counters. In
// shadow/full mode the fast path proves "no other writer committed since our
// last build" with (unified-row counters === our token) + (envelope mtime not
// newer than our build) and skips the read. These pins cover the collapse AND
// the three escape hatches (cross-instance commit, foreign file write, legacy
// exactness).

function makeSession(id: string): Session {
    return {
        id,
        meta: { protocol: "openai", upstreamOrigin: "http://upstream" },
        stats: { requests: 0, tokensSaved: 0, inputTokens: 0, cachedTokens: 0, outputTokens: 0, cacheSamples: 0, lastInputTokens: 0, contextTokens: 0, compressCreditTokens: 0, retrieveCalls: 0, retrieveHits: 0, retrieveMisses: 0, storedBytes: 0, storeBytesSaved: 0, rangeRestores: 0 },
        metadata: {},
        state: createInitialState(),
        createdAt: Date.now(),
        lastSeen: Date.now(),
        blockContents: new Map(),
        inFlight: 0,
        persisted: false,
        pendingRetrievals: [],
    };
}

function withMode(mode: "shadow" | "legacy", name: string, fn: (dir: string) => Promise<void> | void): Promise<void> {
    return test(name, { timeout: 120_000 }, async () => {
        const prev = process.env.BILI_STORAGE_UNIFIED;
        process.env.BILI_STORAGE_UNIFIED = mode === "shadow" ? "1" : "legacy";
        const dir = mkdtempSync(join(tmpdir(), `bili-guard-own-${mode}-`));
        try {
            await fn(dir);
        } finally {
            rmrf(dir);
            if (prev === undefined) delete process.env.BILI_STORAGE_UNIFIED;
            else process.env.BILI_STORAGE_UNIFIED = prev;
        }
    }) as unknown as Promise<void>;
}

function envelopeFileOf(dir: string, id: string): string {
    for (const d of readdirSync(dir, { withFileTypes: true }).filter((e) => e.isDirectory() && e.name !== "index.db")) {
        for (const f of readdirSync(join(dir, d.name))) {
            if (!f.endsWith(".json") || f.endsWith(".content-store.json") || f.includes(".tmp-")) continue;
            const p = join(dir, d.name, f);
            const parsed = JSON.parse(readFileSync(p, "utf8")) as { payload: { id: string } };
            if (parsed.payload.id === id) return p;
        }
    }
    throw new Error(`no envelope for ${id} under ${dir}`);
}

await withMode("shadow", "guard ownership: single-instance saves collapse to one full read; counters stay exact", async (dir) => {
    const store = new SessionStore({ dir, debounceMs: 1, enabled: true, log: () => {} });
    const s = makeSession("own-fast");
    const SAVES = 12;
    for (let i = 1; i <= SAVES; i++) {
        s.stats.requests = i;
        s.metadata.peak = `round-${i}`;
        await store.writeNow(s);
    }
    const file = envelopeFileOf(dir, "own-fast");
    const payload = (JSON.parse(readFileSync(file, "utf8")) as { payload: { stats: { requests: number }; metadata: { peak: string } } }).payload;
    assert.equal(payload.stats.requests, SAVES, "counters on disk track every save");
    assert.equal(payload.metadata.peak, `round-${SAVES}`);
    const fullReads = store.__staleGuardFullReadsForTests();
    assert.ok(fullReads <= 2, `fast path must collapse full reads (got ${fullReads} for ${SAVES} saves)`);
    store.closeQuietly();
});

await withMode("legacy", "guard ownership: legacy mode stays exact — one full read per save", async (dir) => {
    const store = new SessionStore({ dir, debounceMs: 1, enabled: true, log: () => {} });
    const s = makeSession("legacy-exact");
    const SAVES = 5;
    for (let i = 1; i <= SAVES; i++) {
        s.stats.requests = i;
        await store.writeNow(s);
    }
    assert.equal(store.__staleGuardFullReadsForTests(), SAVES, "no sidecar → no fast path → exact full read every save");
    store.closeQuietly();
});

await withMode("shadow", "guard ownership: a sibling instance's commit is detected and the stale write rejected", async (dir) => {
    const warnings: string[] = [];
    const log = (level: string, msg: string): void => {
        if (level === "warn") warnings.push(msg);
    };
    const storeA = new SessionStore({ dir, debounceMs: 1, enabled: true, log });
    const storeB = new SessionStore({ dir, debounceMs: 1, enabled: true, log });
    const a = makeSession("sib");
    a.stats.requests = 5;
    a.metadata.lineage = "A";
    await storeA.writeNow(a);

    // B commits a newer lineage through the SHARED index — the row counters
    // move past A's token, so A's fast path must fall back to the exact read.
    const b = makeSession("sib");
    b.stats.requests = 6;
    b.metadata.lineage = "B-newer";
    await storeB.writeNow(b);

    a.stats.requests = 5; // A is now stale but its counters did not regress
    await storeA.writeNow(a);

    const payload = (JSON.parse(readFileSync(envelopeFileOf(dir, "sib"), "utf8")) as { payload: { stats: { requests: number }; metadata: { lineage: string } } }).payload;
    assert.equal(payload.stats.requests, 6, "disk keeps the sibling's fresher counter");
    assert.equal(payload.metadata.lineage, "B-newer", "disk keeps the sibling's fresher lineage");
    assert.ok(warnings.some((w) => w.includes("rejected stale snapshot")), "the cross-instance catch-up still warns");
    storeA.closeQuietly();
    storeB.closeQuietly();
});

await withMode("shadow", "guard ownership: a foreign file write (legacy sibling / manual edit) trips the mtime check", async (dir) => {
    const warnings: string[] = [];
    const log = (level: string, msg: string): void => {
        if (level === "warn") warnings.push(msg);
    };
    const store = new SessionStore({ dir, debounceMs: 1, enabled: true, log });
    const s = makeSession("foreign");
    s.stats.requests = 3;
    await store.writeNow(s);

    // Push the foreign write outside the ownership grace window so the mtime
    // check is deterministic in-test (1.5s slack for our own write latency).
    sleepSync(1700);
    const file = envelopeFileOf(dir, "foreign");
    const parsed = JSON.parse(readFileSync(file, "utf8")) as { payload: { stats: { requests: number }; metadata: Record<string, unknown> } };
    parsed.payload.stats.requests = 7; // a legacy sibling that never touches index.db
    parsed.payload.metadata.lineage = "foreign-newer";
    writeFileSync(file, JSON.stringify(parsed));

    // Same in-memory counters as before — the guard must still see the file.
    s.stats.requests = 3;
    await store.writeNow(s);

    const after = (JSON.parse(readFileSync(envelopeFileOf(dir, "foreign"), "utf8")) as { payload: { stats: { requests: number }; metadata: { lineage: string } } }).payload;
    assert.equal(after.stats.requests, 7, "disk keeps the foreign fresher counter");
    assert.equal(after.metadata.lineage, "foreign-newer");
    assert.ok(warnings.some((w) => w.includes("rejected stale snapshot")), "foreign write detection still warns");
    store.closeQuietly();
});

await withMode("shadow", "guard ownership: interleaved storm stays exact with the sidecar shared", async (dir) => {
    const warnings: string[] = [];
    const log = (level: string, msg: string): void => {
        if (level === "warn") warnings.push(msg);
    };
    const storeA = new SessionStore({ dir, debounceMs: 1, enabled: true, log });
    const storeB = new SessionStore({ dir, debounceMs: 1, enabled: true, log });
    const a = makeSession("storm");
    const b = makeSession("storm");
    for (let i = 1; i <= 30; i++) {
        a.stats.requests = i;
        a.metadata.peak = `a-${i}`;
        await storeA.writeNow(a);
        if (i % 6 === 0) {
            b.stats.requests = Math.max(0, i - 10); // always behind
            b.metadata.peak = `b-${i}`;
            await storeB.writeNow(b);
        }
    }
    const final = (JSON.parse(readFileSync(envelopeFileOf(dir, "storm"), "utf8")) as { payload: { stats: { requests: number }; metadata: { peak: string } } }).payload;
    assert.equal(final.stats.requests, 30, "storm: disk counter is the freshest lineage");
    assert.equal(final.metadata.peak, "a-30");
    assert.ok(warnings.some((w) => w.includes("rejected stale snapshot")), "lagging writes are still rejected under the fast path");
    storeA.closeQuietly();
    storeB.closeQuietly();
});
