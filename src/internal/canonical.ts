/**
 * Strict bounded JSON framing + canonical HMAC encoding.
 *
 * We deliberately do NOT claim RFC-8785 canonical JSON. Instead we define one
 * unambiguous, bounded encoding:
 *   - a single top-level object;
 *   - lexicographically ordered string keys (byte order);
 *   - duplicate keys are rejected (parsing is strict, not last-wins);
 *   - numbers are only finite safe integers serialized as JSON numbers;
 *   - bigint is serialized as a canonical decimal string (tagged by callers);
 *   - a maximum nesting depth and maximum object/array size bound memory;
 *   - `undefined` values are rejected (callers must omit optional fields).
 *
 * The HMAC material built from this encoding is deterministic and unambiguous;
 * it is tested for that property rather than asserted to be RFC-canonical.
 */

export type JsonValue =
  | null
  | boolean
  | number
  | string
  | bigint
  | JsonValue[]
  | { readonly [key: string]: JsonValue };

export const MAX_DEPTH = 32;
export const MAX_CONTAINER_KEYS = 1024;

export function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Recursively canonicalize a JS value into a JSON-safe tree, sorting object keys
 * bytewise at each level. `undefined` is rejected (optional wire fields must be
 * omitted, never emitted as undefined). bigint is retained and serialized as a
 * decimal string by `canonicalSerialize`. Non-finite numbers are rejected.
 */
export function canonicalize(value: unknown, depth = 0): JsonValue {
  if (depth > MAX_DEPTH) throw new Error(`nesting deeper than ${MAX_DEPTH}`);
  if (value === null) return null;
  if (typeof value === "boolean") return value;
  if (typeof value === "string") return value;
  if (typeof value === "bigint") return value.toString();
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new Error("non-finite number cannot be encoded");
    if (!Number.isSafeInteger(value)) throw new Error("number must be a safe integer");
    return value;
  }
  if (typeof value === "undefined") {
    throw new Error("undefined cannot be encoded; omit optional fields instead");
  }
  if (Array.isArray(value)) {
    if (value.length > MAX_CONTAINER_KEYS) throw new Error("array too large to encode");
    return value.map((item) => canonicalize(item, depth + 1));
  }
  if (typeof value === "object") {
    const obj = value as Record<string, unknown>;
    const keys = Object.keys(obj).sort();
    if (keys.length > MAX_CONTAINER_KEYS) throw new Error("object too large to encode");
    const out: Record<string, JsonValue> = {};
    for (const key of keys) out[key] = canonicalize(obj[key], depth + 1);
    return out;
  }
  throw new Error(`cannot encode value of type ${typeof value}`);
}

/** Serialize a value to the bounded canonical byte form (deterministic HMAC input). */
export function canonicalSerialize(value: unknown): string {
  return JSON.stringify(canonicalize(value));
}

export interface CanonicalObject {
  readonly [key: string]: unknown;
}

interface ParseState {
  input: string;
  pos: number;
}

function skipWs(s: ParseState): void {
  while (s.pos < s.input.length) {
    const ch = s.input[s.pos];
    if (ch === " " || ch === "\t" || ch === "\n" || ch === "\r") s.pos += 1;
    else break;
  }
}

function parseString(s: ParseState): string {
  if (s.input[s.pos] !== '"') throw new Error("expected string");
  s.pos += 1;
  let out = "";
  while (s.pos < s.input.length) {
    const ch = s.input[s.pos];
    if (ch === '"') {
      s.pos += 1;
      return out;
    }
    if (ch === "\\") {
      s.pos += 1;
      if (s.pos >= s.input.length) throw new Error("truncated escape");
      const esc = s.input[s.pos];
      switch (esc) {
        case '"': out += '"'; s.pos += 1; break;
        case "\\": out += "\\"; s.pos += 1; break;
        case "/": out += "/"; s.pos += 1; break;
        case "b": out += "\b"; s.pos += 1; break;
        case "f": out += "\f"; s.pos += 1; break;
        case "n": out += "\n"; s.pos += 1; break;
        case "r": out += "\r"; s.pos += 1; break;
        case "t": out += "\t"; s.pos += 1; break;
        case "u": {
          if (s.pos + 4 >= s.input.length) throw new Error("truncated unicode escape");
          const hex = s.input.slice(s.pos + 1, s.pos + 5);
          if (!/^[0-9a-fA-F]{4}$/.test(hex)) throw new Error("invalid unicode escape");
          out += String.fromCharCode(parseInt(hex, 16));
          s.pos += 5;
          break;
        }
        default:
          throw new Error("invalid escape sequence");
      }
    }
    else {
      const code = ch!.codePointAt(0)!;
      if (code < 0x20) throw new Error("unescaped control character in string");
      out += ch!;
      s.pos += 1;
    }
  }
  throw new Error("unterminated string");
}

function parseNumber(s: ParseState): number {
  const start = s.pos;
  if (s.input[s.pos] === "-") s.pos += 1;
  if (s.pos >= s.input.length) throw new Error("invalid number");
  if (s.input[s.pos] === "0") {
    s.pos += 1;
    const next = s.input[s.pos];
    if (next !== undefined && /[0-9]/.test(next)) throw new Error("leading zeros not allowed");
  }
  else {
    if (!/[1-9]/.test(s.input[s.pos]!)) throw new Error("invalid number");
    let c = s.input[s.pos];
    while (c !== undefined && /[0-9]/.test(c)) { s.pos += 1; c = s.input[s.pos]; }
  }
  // Reject fraction / exponent: only canonical integer numbers allowed.
  const after = s.input[s.pos];
  if (after !== undefined && (after === "." || after === "e" || after === "E")) {
    throw new Error("only integer numbers are allowed");
  }
  const text = s.input.slice(start, s.pos);
  const value = Number(text);
  if (!Number.isSafeInteger(value)) throw new Error("number out of safe integer range");
  if (Object.is(value, -0)) throw new Error("negative zero not allowed");
  return value;
}

function parseValue(s: ParseState, depth: number): unknown {
  if (depth > MAX_DEPTH) throw new Error("nesting too deep");
  skipWs(s);
  if (s.pos >= s.input.length) throw new Error("unexpected end of input");
  const ch = s.input[s.pos];
  if (ch === "{") return parseObject(s, depth);
  if (ch === "[") return parseArray(s, depth);
  if (ch === '"') return parseString(s);
  if (ch === "t") {
    if (s.input.startsWith("true", s.pos)) { s.pos += 4; return true; }
    throw new Error("invalid literal");
  }
  if (ch === "f") {
    if (s.input.startsWith("false", s.pos)) { s.pos += 5; return false; }
    throw new Error("invalid literal");
  }
  if (ch === "n") {
    if (s.input.startsWith("null", s.pos)) { s.pos += 4; return null; }
    throw new Error("invalid literal");
  }
  if (ch === "-" || /[0-9]/.test(ch!)) return parseNumber(s);
  throw new Error("invalid token");
}

function parseObject(s: ParseState, depth: number): Record<string, unknown> {
  s.pos += 1; // {
  const out: Record<string, unknown> = {};
  let count = 0;
  skipWs(s);
  if (s.input[s.pos] === "}") { s.pos += 1; return out; }
  for (;;) {
    if (count >= MAX_CONTAINER_KEYS) throw new Error("object too large");
    skipWs(s);
    if (s.input[s.pos] !== '"') throw new Error("expected object key");
    const key = parseString(s);
    if (Object.prototype.hasOwnProperty.call(out, key)) throw new Error(`duplicate key: ${key}`);
    skipWs(s);
    if (s.input[s.pos] !== ":") throw new Error("expected ':'");
    s.pos += 1;
    out[key] = parseValue(s, depth + 1);
    count += 1;
    skipWs(s);
    const sep = s.input[s.pos];
    if (sep === ",") { s.pos += 1; continue; }
    if (sep === "}") { s.pos += 1; return out; }
    throw new Error("expected ',' or '}'");
  }
}

function parseArray(s: ParseState, depth: number): unknown[] {
  s.pos += 1; // [
  const out: unknown[] = [];
  skipWs(s);
  if (s.input[s.pos] === "]") { s.pos += 1; return out; }
  for (;;) {
    if (out.length >= MAX_CONTAINER_KEYS) throw new Error("array too large");
    out.push(parseValue(s, depth + 1));
    skipWs(s);
    const sep = s.input[s.pos];
    if (sep === ",") { s.pos += 1; continue; }
    if (sep === "]") { s.pos += 1; return out; }
    throw new Error("expected ',' or ']'");
  }
}

/**
 * Parse a strictly-encoded canonical object from its byte string. Rejects
 * duplicate keys, non-canonical number spellings, non-integer/fraction/exponent
 * numbers, non-safe integers, negative zero, and excess nesting/size. Returns a
 * plain object whose values are string/boolean/null/safe-integer/array/object.
 */
export function parseCanonicalObject(input: string): CanonicalObject {
  const s: ParseState = { input, pos: 0 };
  const value = parseValue(s, 0);
  skipWs(s);
  if (s.pos !== input.length) throw new Error("trailing characters after payload");
  if (!isPlainObject(value)) throw new Error("frame payload must be a single JSON object");
  const obj = value as Record<string, unknown>;
  // The accepted bytes must be exactly what our serializer emits. This rejects
  // whitespace, alternate escapes, and non-canonical key order in addition to
  // the duplicate-key and number checks performed while parsing.
  if (canonicalSerialize(obj) !== input) {
    throw new Error("payload is not canonical");
  }
  return obj as CanonicalObject;
}

/** Validate that a string is a canonical decimal string for a 64-bit integer. */
export function isDecimalIntString(value: unknown): value is string {
  if (typeof value !== "string") return false;
  if (!/^-?(0|[1-9][0-9]*)$/.test(value)) return false;
  if (value === "-0") return false;
  try {
    const n = BigInt(value);
    return n >= -9223372036854775808n && n <= 9223372036854775807n;
  }
  catch {
    return false;
  }
}
