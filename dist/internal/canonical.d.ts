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
export type JsonValue = null | boolean | number | string | bigint | JsonValue[] | {
    readonly [key: string]: JsonValue;
};
export declare const MAX_DEPTH = 32;
export declare const MAX_CONTAINER_KEYS = 1024;
export declare function isPlainObject(value: unknown): value is Record<string, unknown>;
/**
 * Recursively canonicalize a JS value into a JSON-safe tree, sorting object keys
 * bytewise at each level. `undefined` is rejected (optional wire fields must be
 * omitted, never emitted as undefined). bigint is retained and serialized as a
 * decimal string by `canonicalSerialize`. Non-finite numbers are rejected.
 */
export declare function canonicalize(value: unknown, depth?: number): JsonValue;
/** Serialize a value to the bounded canonical byte form (deterministic HMAC input). */
export declare function canonicalSerialize(value: unknown): string;
export interface CanonicalObject {
    readonly [key: string]: unknown;
}
/**
 * Parse a strictly-encoded canonical object from its byte string. Rejects
 * duplicate keys, non-canonical number spellings, non-integer/fraction/exponent
 * numbers, non-safe integers, negative zero, and excess nesting/size. Returns a
 * plain object whose values are string/boolean/null/safe-integer/array/object.
 */
export declare function parseCanonicalObject(input: string): CanonicalObject;
/** Validate that a string is a canonical decimal string for a 64-bit integer. */
export declare function isDecimalIntString(value: unknown): value is string;
