# pi-extension-utils

Shared utilities for Pi extensions. The package exposes independent entry points and has no runtime dependencies or native bindings.

## Process domain

`pi-extension-utils/process-domain` provides one root-owned local transport and inherited child nodes:

- framed messaging over loopback TCP (`node:net`) with directed sends, broadcast, receive acknowledgements, and peer events;
- application-level ping/pong heartbeat options for liveness, so a frozen peer is reported even though the kernel keeps its connection open;
- Pi lifecycle forwarding without imposing a watchdog state machine;
- one listener on an ephemeral loopback port (`tcp://127.0.0.1:*`); every peer is one exact connection, so a disconnect maps to one exact node;
- an inherited authenticated declaration in `PI_EXTENSION_UTILS_PROCESS_DOMAIN`.

The wire format is a 4-byte big-endian length prefix carrying a strictly validated JSON envelope (64 KiB bound, fail-closed). Authentication is a per-connection HMAC challenge-response: fresh nonces on every connection make consumed proofs useless for replay, and a reconnecting peer fences and replaces its stale incarnation. Disconnected clients retry connection every fixed 1 second, open a brand-new connection, and rerun the full handshake; pending sends fail instead of being replayed. Consumers receive the online peer event and must publish any newly queried business state themselves. A receipt acknowledgement resolves once the message reaches the target peer; routed messages acknowledge only after every downstream hop acknowledged.

```ts
import { attachPiLifecycle, openProcessDomain } from "pi-extension-utils/process-domain";

const node = await openProcessDomain({
  metadata: { extension: "pi-reflect-watchdog" },
});

attachPiLifecycle(node, pi, sessionId);
node.subscribe("work", (message) => console.log(message.senderId, message.value));
await node.broadcast("work", { kind: "turn-end" });
```

The root is the only node that binds endpoints. Children only connect to inherited endpoints. The transport does not own counters, retry policies, idle decisions, watchdog thresholds, or durable delivery.

Startup failures are exposed as `ProcessDomainOpenError` with a stable `code`: `INVALID_DECLARATION`, `AUTHENTICATION_FAILED`, or `CONNECTION_UNAVAILABLE`. Consumers should branch only through `isProcessDomainOpenError()` and `code`; the public error message is sanitized and is not a classification contract.

## XML

`pi-extension-utils/xml` builds and strictly parses one unique trailing XML document, extracts non-thinking assistant text, and neutralizes a finalized assistant message without importing Pi.

```ts
import { buildXmlDocument, parseTrailingXml } from "pi-extension-utils/xml";

const example = buildXmlDocument("reflection", [
  { name: "type", value: "NO_ISSUE" },
]);
const result = parseTrailingXml(`Checked.\n${example}`, "reflection");
```

## Pi inquiry

`pi-extension-utils/pi-inquiry` creates correlated hidden inquiry and fold messages, neutralizes inquiry assistants, and registers provider-context folding. Business validation and retry policy remain in the consuming extension.

`createInquiryRuntime(namespace).attempt(n)` returns one pure terminal handle with immutable correlation and `pending | sent | completed | cancelled` state. It matches only its exact prompt, blocks capture after terminal cancellation, makes completion first-terminal-wins, and makes cancellation idempotently return the same remove-fold for retry. Its `neutralize()` helper clears assistant content while preserving exact correlation. Context folding removes an aborted exchange without a fold marker only when the assistant was neutralized with that exact correlation. The utility does not import Pi types, call `ctx.abort()`, enqueue user input, or decide business outcomes.

## Runtime support

process-domain relies only on the portable `node:net`/`node:crypto` surface and runs the same TypeScript on Node.js, Bun, and Deno, including their standalone compiled forms. The same full liveness/reconnect acceptance file runs under all three hosts (`test:acceptance`, `test:acceptance:bun`, and `test:acceptance:deno`). The Deno harness uses `Deno.kill()` for Unix stop/continue signals because Deno 2.9.5's `node:child_process` shim incorrectly treats `killed` as a one-shot guard: after a successful `SIGSTOP`, `ChildProcess.kill("SIGCONT")` returns `false` without sending the signal. This is a Deno Node-compatibility bug, not a transport limitation. Compiled Bun and Deno host startup smoke tests are also verified.

## Development

```sh
npm ci
npm run check
```
