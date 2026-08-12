import { spawn } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { describe, expect, it } from "vitest";
import { ENV_NAMES, FATAL_EXIT_CODE, openDomain } from "../../src/index.js";

function clearDeclaration(): void {
  for (const name of Object.values(ENV_NAMES)) delete process.env[name];
  process.exitCode = undefined;
}

describe("embedded root ownership", () => {
  for (const [label, mutate] of [
    ["domain id", (value: string) => `${value}x`],
    ["domain key", () => Buffer.alloc(32, 9).toString("base64url")],
    ["protocol", () => "1.0"],
  ] as const) {
    it(`rejects a mismatched ${label}`, async () => {
      clearDeclaration();
      const first = await openDomain({ onFatal: () => {} });
      const name = label === "domain id" ? ENV_NAMES.DOMAIN_ID : label === "domain key" ? ENV_NAMES.DOMAIN_KEY : ENV_NAMES.PROTOCOL;
      const original = process.env[name]!;
      process.env[name] = mutate(original);
      await expect(openDomain({ onFatal: () => {} })).rejects.toMatchObject({ code: "INVALID_DECLARATION" });
      expect(process.exitCode).toBe(FATAL_EXIT_CODE);
      process.env[name] = original;
      await first.domain.close();
    });
  }

  it("serializes a final close with the next open", async () => {
    clearDeclaration();
    const first = await openDomain({ onFatal: () => {} });
    const close = first.domain.close();
    const second = await openDomain({ onFatal: () => {} });
    await close;
    expect(second.created || second.domain.snapshot().certain).toBe(true);
    expect(second.domain.snapshot().certain).toBe(true);
    expect(process.env[ENV_NAMES.DOMAIN_ID]).toBe(second.domain.snapshot().domainId);
    await second.domain.close();
  });

  it("closes under Bun after an inherited child leaves", async () => {
    const bun = process.env.BUN_EXE;
    if (bun === undefined) return;
    const child = spawn(bun, [resolve("test/harness/embedded-close.mjs")], { stdio: "ignore" });
    const status = await new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve, reject) => {
      child.once("error", reject);
      child.once("exit", (code, signal) => resolve({ code, signal }));
    });
    expect(status).toEqual({ code: 0, signal: null });
  }, 5_000);

  it("opens and closes a fresh root with a long runtime base", async () => {
    clearDeclaration();
    const previousXdg = process.env.XDG_RUNTIME_DIR;
    const base = await mkdtemp(join(tmpdir(), "pi-process-domain-long-"));
    process.env.XDG_RUNTIME_DIR = join(base, "nested-runtime-component".repeat(8));
    try {
      const opened = await openDomain({ onFatal: () => {} });
      expect(opened.created).toBe(true);
      expect(opened.domain.snapshot().certain).toBe(true);
      await opened.domain.close();
    }
    finally {
      clearDeclaration();
      if (previousXdg === undefined) delete process.env.XDG_RUNTIME_DIR;
      else process.env.XDG_RUNTIME_DIR = previousXdg;
      await rm(base, { recursive: true, force: true });
    }
  });
});
