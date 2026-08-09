import { beforeAll, afterAll, afterEach, describe, expect, it } from "vitest";
import { ENV, createDeclaration, readDeclaration } from "../../src/internal/declaration.js";
import { base64url } from "../../src/internal/auth.js";
import { ProcessDomainFatalError } from "../../src/internal/errors.js";

describe("declaration", () => {
  const saved: Record<string, string | undefined> = {};

  beforeAll(() => {
    for (const k of Object.values(ENV)) {
      saved[k] = process.env[k];
      delete process.env[k];
    }
  });

  afterAll(() => {
    for (const k of Object.values(ENV)) {
      if (saved[k] === undefined) delete process.env[k];
      else process.env[k] = saved[k];
    }
  });

  afterEach(() => {
    for (const k of Object.values(ENV)) delete process.env[k];
  });

  it("returns null when no declaration is present", () => {
    expect(readDeclaration()).toBeNull();
  });

  it("createDeclaration writes a valid 32-byte-key declaration and round-trips", () => {
    const decl = createDeclaration();
    expect(decl.domainId).toMatch(/^[A-Za-z0-9_-]{8,}$/);
    expect(decl.domainKey.length).toBe(32);
    const read = readDeclaration();
    expect(read).not.toBeNull();
    expect(read!.domainId).toBe(decl.domainId);
    expect(Buffer.from(read!.domainKey).equals(Buffer.from(decl.domainKey))).toBe(true);
    expect(read!.protocolMajor).toBe(2);
    expect(read!.protocolMinor).toBe(0);
  });

  it("fails closed on a partial declaration", () => {
    process.env[ENV.DOMAIN_ID] = "abc";
    expect(() => readDeclaration()).toThrow(ProcessDomainFatalError);
  });

  it("fails closed on a wrong-length key", () => {
    process.env[ENV.DOMAIN_ID] = "domain-one-abc";
    process.env[ENV.DOMAIN_KEY] = base64url(new Uint8Array(16));
    process.env[ENV.PROTOCOL] = "1.0";
    expect(() => readDeclaration()).toThrow(ProcessDomainFatalError);
  });

  it("fails closed on an unsupported protocol version", () => {
    process.env[ENV.DOMAIN_ID] = "domain-two-abc";
    process.env[ENV.DOMAIN_KEY] = base64url(new Uint8Array(32).fill(1));
    process.env[ENV.PROTOCOL] = "999.0";
    expect(() => readDeclaration()).toThrow(/incompatible/i);

    process.env[ENV.PROTOCOL] = "1.999";
    expect(() => readDeclaration()).toThrow(/incompatible/i);
  });

  it("retains protocol 1.0 only as a legacy declared-session topology", () => {
    process.env[ENV.DOMAIN_ID] = "legacy-domain-abc";
    process.env[ENV.DOMAIN_KEY] = base64url(new Uint8Array(32).fill(1));
    process.env[ENV.PROTOCOL] = "1.0";
    expect(readDeclaration()?.protocolMajor).toBe(1);
  });
});
