/**
 * HMAC-SHA-256 challenge-response authentication and key derivation.
 *
 * The broker stores the derived per-domain authentication key, never the raw
 * environment key. Both derive the same key via HKDF-SHA-256. Distinct labels
 * bind the client and server MACs so each side proves it holds the same derived
 * domain key. All MAC comparisons are constant-time fixed-length.
 */
export declare const AUTH_KEY_BYTES = 32;
export declare const NONCE_BYTES = 16;
export declare const MAC_BYTES = 32;
/** Derive the per-domain authentication key from the 32-byte environment key. */
export declare function deriveDomainAuthKey(domainKey: Uint8Array, domainId: string): Uint8Array;
export declare function randomNonce(): Uint8Array;
export declare function randomToken(bytes?: number): Uint8Array;
export declare function clientMac(domainAuthKey: Uint8Array, domainId: string, clientNonce: Uint8Array, brokerNonce: Uint8Array, brokerEpoch: string): Buffer;
export declare function serverMac(domainAuthKey: Uint8Array, domainId: string, clientNonce: Uint8Array, brokerNonce: Uint8Array, brokerEpoch: string): Buffer;
export declare function macEqual(a: Uint8Array, b: Uint8Array): boolean;
export declare function sha256Hex(value: string): string;
export declare function base64url(data: Uint8Array | Buffer): string;
export declare function unbase64url(data: string, expectedBytes?: number): Uint8Array | null;
