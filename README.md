# pi-process-domain

Authenticated cross-process coordination for Pi extensions and other Node.js processes that must make exact aggregate-idle decisions.

The package launches one private per-user broker on demand. Processes in a domain authenticate with a 32-byte environment capability, register exact participant leases, publish busy/idle state, reserve child launches before spawning, and confirm immutable idle fences. A snapshot can report `allIdle: true` only while broker membership is certain, at least one participant exists, every participant is idle, and no spawn reservation is pending.

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

The first process with no declaration creates a random domain and writes these variables to its own `process.env` for descendants:

- `PI_PROCESS_DOMAIN_ID`
- `PI_PROCESS_DOMAIN_KEY`
- `PI_PROCESS_DOMAIN_PROTOCOL`

Any partial or malformed declaration fails closed. Protocol v1 supports exactly version `1.0`; any other major or minor rejects with `PROTOCOL_MISMATCH`. A declared domain with a wrong key, absent broker state, rejected lease, or incompatible protocol rejects `openDomain()` with `ProcessDomainFatalError`. Initial `openDomain()` failures preserve fail-closed process status by setting `process.exitCode` to `FATAL_EXIT_CODE` (78), even when the rejection is caught. Applications may provide `onFatal` to replace default logging and receive the error, but not to opt out of the initial fail-closed exit status.

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

A broker restart creates a new epoch. Old fences never validate. Since v1 has no durable membership journal, recovery deliberately remains uncertain for the lease recovery window and until an authenticated participant resumes; it never guesses that missing participants are idle. Reservations lost with a crashed broker are likewise uncertainty, not proof of idle.

## Failure model

- Initial open is bounded by `connectTimeoutMs` (default 10 seconds) and resolves only after authenticated lease registration.
- Reconnect preserves participant ID through a broker-issued resume capability and strictly increasing incarnation.
- A stale/equal incarnation is rejected; a newer exact incarnation supersedes the old connection.
- Real client heartbeats are broker liveness evidence. Suspect/disconnected leases make the domain uncertain until they resume or expire.
- Malformed, oversized, or non-canonical wire frames are rejected and the peer is closed.
- Runtime directories and Unix socket paths are private per-user paths; unsafe/symlinked configured runtime paths fail closed.

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

`npm run check` runs typecheck, lint, rebuilds the committed distribution artifacts, and runs unit tests. Acceptance tests launch the actual public `openDomain()` and broker in finite-time subprocess scenarios, then verify a clean packed install/import. CI rejects every commit whose regenerated `dist/` differs from the committed artifacts.

## Security notes

The environment key is a bearer capability and must not be logged or exposed to unrelated processes. Authentication protects domains from other broker clients that do not possess the key; it does not sandbox code already running as the same OS user. Domain state is intentionally in memory and is not a durable job database.

MIT licensed. Vendored birpc attribution is in [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
