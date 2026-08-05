#!/usr/bin/env node
/**
 * Broker entrypoint (detached, launched lazily by a client).
 *
 * Imports the compiled broker from `dist`. It is an implementation detail of
 * the package (not a user-managed daemon). A build step produces `dist` before
 * broker launch. The path is resolved relative to this file so it works from a
 * committed `dist` in a clean Git install and from node_modules.
 */
async function main() {
  const { launchBrokerForCurrentUser } = await import(new URL("../dist/internal/broker.js", import.meta.url));
  const { ELECTION_OWNER_ENV } = await import(new URL("../dist/internal/launcher.js", import.meta.url));
  const electionOwner = process.env[ELECTION_OWNER_ENV];
  if (!electionOwner) throw new Error("missing broker election ownership token");
  delete process.env[ELECTION_OWNER_ENV];
  await launchBrokerForCurrentUser(electionOwner);
  // Keep this broker process alive. It was spawned detached and unref'ed by
  // the launching client, so it does not hold the parent Pi process alive;
  // within this process the interval is referenced so the broker persists.
  setInterval(() => {}, 60_000);
}

main().catch((err) => {
  console.error("pi-process-domain broker failed to start:", err?.message ?? err);
  process.exit(78);
});
