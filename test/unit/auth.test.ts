import { describe, expect, it } from "vitest";
import { base64url, deriveDomainAuthKey, macEqual, unbase64url } from "../../src/internal/auth.js";

describe("auth primitives", () => {
  it("derives a deterministic 32-byte key per domain id", () => {
    const key = new Uint8Array(32).fill(7);
    const a = deriveDomainAuthKey(key, "domain-a");
    const b = deriveDomainAuthKey(key, "domain-b");
    const again = deriveDomainAuthKey(key, "domain-a");
    expect(a.length).toBe(32);
    expect(Buffer.from(a).equals(Buffer.from(again))).toBe(true);
    expect(Buffer.from(a).equals(Buffer.from(b))).toBe(false);
  });

  it("base64url round-trips and rejects wrong-length inputs", () => {
    const bytes = new Uint8Array([1, 2, 3, 250]);
    const enc = base64url(bytes);
    const dec = unbase64url(enc, 4);
    expect(Buffer.from(dec as Uint8Array).equals(Buffer.from(bytes))).toBe(true);
    expect(unbase64url(enc, 5)).toBeNull();
    expect(unbase64url("!!!", 4)).toBeNull();
  });

  it("macEqual is false for mismatched lengths or bytes", () => {
    const a = new Uint8Array([1, 2, 3]);
    const b = new Uint8Array([1, 2, 3]);
    const c = new Uint8Array([1, 2, 4]);
    const d = new Uint8Array([1, 2]);
    expect(macEqual(a, b)).toBe(true);
    expect(macEqual(a, c)).toBe(false);
    expect(macEqual(a, d)).toBe(false);
  });
});
