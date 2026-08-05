import { describe, expect, it } from "vitest";
import {
  canonicalSerialize,
  isDecimalIntString,
  parseCanonicalObject,
} from "../../src/internal/canonical.js";

describe("canonical JSON", () => {
  it("sorts keys lexicographically", () => {
    const out = canonicalSerialize({ z: 1, a: 2, m: 3 });
    expect(out).toBe('{"a":2,"m":3,"z":1}');
  });

  it("rejects non-object roots and non-canonical input", () => {
    expect(() => parseCanonicalObject("[1,2]")).toThrow(/single JSON object/);
    // out-of-order keys fail the canonical re-encoding check
    expect(() => parseCanonicalObject('{"b":1,"a":2}')).toThrow(/not canonical/);
  });

  it("accepts a canonical object and rejects alternate bytes", () => {
    expect(parseCanonicalObject('{"a":1,"b":2}')).toEqual({ a: 1, b: 2 });
    expect(() => parseCanonicalObject('{ "a":1}')).toThrow(/not canonical/);
    expect(() => parseCanonicalObject('{"a":1,"a":2}')).toThrow(/duplicate/);
    expect(() => parseCanonicalObject('{"a":1e0}')).toThrow(/integer/);
  });

  it("serializes bigint as a canonical decimal string", () => {
    expect(canonicalSerialize({ value: 42n })).toBe('{"value":"42"}');
  });

  it("identifies canonical decimal integer strings", () => {
    expect(isDecimalIntString("42")).toBe(true);
    expect(isDecimalIntString("-7")).toBe(true);
    expect(isDecimalIntString("0")).toBe(true);
    expect(isDecimalIntString("4.2")).toBe(false);
    expect(isDecimalIntString("abc")).toBe(false);
  });
});
