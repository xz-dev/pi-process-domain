# pi-process-domain

Authenticated cross-process coordination for Pi extensions and other Node.js processes that must make exact aggregate-idle decisions.

A fresh root hosts one private single-domain broker inside its own process. Descendants inherit a versioned declaration, authenticate with a 32-byte environment capability, and connect to that root's credential-free endpoint. Each root/domain has an isolated Unix socket or Windows named pipe, so one root's lifecycle cannot restart or disturb another. A snapshot can report `allIdle: true` only while broker membership is certain, at least one participant exists, every participant is idle, and no spawn reservation is pending.

## Requirements

- Node.js 22.19 or newer
- Unix domain sockets on Linux/macOS/FreeBSD, or named pipes on Windows

The package ships committed JavaScript and declarations in `dist/`; Git and npm artifact consumers do not need TypeScript or an install-time build.

## Install

```sh
npm install git:github.com/xz-dev/pi-process-domain
```

## Use

```js
import { openDomain } from "pi-process-domain";

const { domain, created } = await openDomain({
  initialActivity: "busy",
  metadata: { role: "main" },
});

await domain.setActivity("idle");
const snapshot = domain.snapshot();
if (snapshot.allIdle && await domain.confirm(snapshot.fence)) {
  // The same broker epoch and activity generation are still certainly idle.
}

await domain.close();
```

The first process with no declaration binds and joins an embedded broker, then atomically publishes these variables to its own `process.env` for descendants:

- `PI_PROCESS_DOMAIN_ID`
- `PI_PROCESS_DOMAIN_KEY`
- `PI_PROCESS_DOMAIN_PROTOCOL`

Fresh roots publish protocol `2.0`. Protocol `1.0` is retained only so declarations inherited by already-running legacy sessions can join an already-running detached per-user broker; public `openDomain()` never creates or revives that legacy broker. Any other major or minor rejects with `PROTOCOL_MISMATCH`.

Any partial or malformed declaration fails closed. A declared domain with a wrong key, absent broker state, rejected lease, or incompatible protocol rejects `openDomain()` with `ProcessDomainFatalError`. Initial `openDomain()` failures preserve fail-closed process status by setting `process.exitCode` to `FATAL_EXIT_CODE` (78), even when the rejection is caught. Applications may provide `onFatal` to replace default logging and receive the error, but not to opt out of the initial fail-closed exit status.

`brokerEndpoint()` returns the endpoint selected by the current declaration: a protocol-2 per-domain endpoint for fresh roots/descendants, the historical per-user endpoint for protocol-1 declarations, and the historical endpoint when called before a declaration exists.

## Reservation-before-spawn

Reserve before launching a child so the aggregate cannot briefly appear idle between spawn and child registration:

```js
const reservation = await domain.reserveSpawn({ ttlMs: 30_000 });
const child = spawn(process.execPath, ["child.mjs"], {
  env: { ...process.env, ...reservation.env },
});

child.once("error", () => void reservation.cancel());
```

The reservation claim is bound to the domain, expires, is single-use, and is mandatory when present. Malformed, canceled, expired, or replayed claims reject the child's join instead of degrading to an ordinary participant.

## Snapshot and fence semantics

`domain.snapshot()` returns immutable counters and:

- `brokerEpoch`: random identity of the current broker/domain state.
- `revision`: monotonically increases for committed state changes in that epoch.
- `activityGeneration`: changes whenever activity, participants, or reservations can invalidate an idle decision.
- `certain`: false during disconnect suspicion, reconnect, and broker-restart recovery.
- `allIdle`: never true while `certain` is false.
- `fence`: `{ brokerEpoch, activityGeneration }` for `domain.confirm(fence)`.

A broker epoch change invalidates old fences. Protocol-2 embedded brokers are owned by the root process: when that root exits, descendants stay uncertain/fail-closed and never create or revive the endpoint. Protocol-1 compatibility recovery retains the existing conservative behavior: with no durable membership journal it remains uncertain for the lease recovery window and until an authenticated participant resumes; it never guesses that missing participants are idle. Reservations lost with a crashed broker are likewise uncertainty, not proof of idle.

## Failure model

- Initial open is bounded by `connectTimeoutMs` (default 10 seconds) and resolves only after authenticated lease registration.
- Reconnect normally preserves participant ID through a broker-issued resume capability and strictly increasing incarnation.
- A protocol-2 descendant only reconnects to its inherited endpoint. Endpoint loss cannot invoke legacy election or broker spawning.
- If an established client's old lease was authoritatively expired, it stays fail-closed/uncertain and re-registers as a fresh participant in the same authenticated domain; runtime lease loss is never emitted as a host-fatal callback.
- A stale/equal incarnation is rejected; a newer exact incarnation supersedes only the old connection for that participant.
- Real client heartbeats are broker liveness evidence. Suspect/disconnected leases make the domain uncertain until they resume or expire.
- Malformed, oversized, or non-canonical wire frames are rejected and the peer is closed.
- Runtime directories are private per-user paths; protocol-2 socket/pipe names include only bounded hashes of public domain IDs, never keys or reservation tokens. Unsafe/symlinked configured runtime paths fail closed.

## Signals

```js
const unsubscribe = domain.subscribeSignals("worker-ready", (signal) => {
  console.log(signal.value, signal.senderId);
});
await domain.publish("worker-ready", { pid: process.pid });
unsubscribe();
```

Signals are bounded best-effort broker broadcasts, not durable messaging.

## Development

```sh
npm ci
npm run check
npm run test:acceptance
```

`npm run check` runs typecheck, lint, rebuilds the committed distribution artifacts, and runs unit and cross-process acceptance tests. Acceptance launches actual public `openDomain()` roots/descendants, verifies root isolation and fail-closed loss, exercises the explicit legacy compatibility broker, then verifies a clean packed install/import. CI rejects every commit whose regenerated `dist/` differs from the committed artifacts.

## Security notes

The environment key is a bearer capability and must not be logged or exposed to unrelated processes. Authentication protects domains from other broker clients that do not possess the key; it does not sandbox code already running as the same OS user. Domain state is intentionally in memory and is not a durable job database.

MIT licensed. Vendored birpc attribution is in [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
