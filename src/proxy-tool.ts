import { RULE_TOOL_NAME, PROXY_TOOL_NAMES } from "./compress-tool.js";
import { ccrEnabled, retrieveToolName } from "./store.js";
import { effectiveRulesEnabled } from "./rules-feature.js";
import type { Config } from "acp-kernel";
import type { Session } from "./session.js";

export function isProxyToolFor(name: string, session: Session | undefined, config: Config): boolean {
    if (PROXY_TOOL_NAMES.has(name)) return true;
    if (ccrEnabled(session) && name === retrieveToolName(session)) return true;
    return effectiveRulesEnabled(session, config) && name === RULE_TOOL_NAME;
}
