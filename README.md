# pi-extension-utils

Shared utilities for Pi extensions. The package exposes independent entry points so XML-only consumers never load the native ZeroMQ binding.

## Process domain

`pi-extension-utils/process-domain` provides one root-owned local transport and inherited child nodes:

- ZeroMQ `ROUTER`/`DEALER` messaging with directed sends, broadcast, receive acknowledgements, and peer events;
- ZMTP heartbeat options for transport liveness; no application heartbeat protocol;
- Pi lifecycle forwarding without imposing a watchdog state machine;
- ZeroMQ-generated temporary endpoints only: `ipc://*` when `zeromq.capability.ipc` is true, otherwise `tcp://127.0.0.1:*`;
- one bootstrap endpoint plus a temporary endpoint per child so a ZeroMQ disconnect maps to one exact node;
- an inherited authenticated declaration in `PI_EXTENSION_UTILS_PROCESS_DOMAIN`.

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

## XML

`pi-extension-utils/xml` builds and strictly parses one unique trailing XML document, extracts non-thinking assistant text, and neutralizes a finalized assistant message without importing Pi or ZeroMQ.

```ts
import { buildXmlDocument, parseTrailingXml } from "pi-extension-utils/xml";

const example = buildXmlDocument("reflection", [
  { name: "type", value: "NO_ISSUE" },
]);
const result = parseTrailingXml(`Checked.\n${example}`, "reflection");
```

## Pi inquiry

`pi-extension-utils/pi-inquiry` creates correlated hidden inquiry and fold messages, neutralizes inquiry assistants, and registers provider-context folding. Business validation and retry state remain in the consuming extension.

## Runtime support

The published `zeromq` package officially targets Node.js through N-API. Other N-API runtimes may work when they can load the installed zeromq native binary, but this package does not claim support beyond the runtimes verified by its acceptance suite. Endpoint selection is based only on `zeromq.capability.ipc`, never on an operating-system name.

## Development

```sh
npm ci
npm run check
```
