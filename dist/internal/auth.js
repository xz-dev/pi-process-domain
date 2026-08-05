/**
 * HMAC-SHA-256 challenge-response authentication and key derivation.
 *
 * The broker stores the derived per-domain authentication key, never the raw
 * environment key. Both derive the same key via HKDF-SHA-256. Distinct labels
 * bind the client and server MACs so each side proves it holds the same derived
 * domain key. All MAC comparisons are constant-time fixed-length.
 */
import { createHmac, createHash, hkdfSync, randomBytes, timingSafeEqual, } from "node:crypto";
export const AUTH_KEY_BYTES = 32;
export const NONCE_BYTES = 16;
export const MAC_BYTES = 32;
const HKDF_INFO = "pi-process-domain/v1/auth";
const CLIENT_LABEL = "pi-process-domain/v1/client";
const SERVER_LABEL = "pi-process-domain/v1/server";
/** Derive the per-domain authentication key from the 32-byte environment key. */
export function deriveDomainAuthKey(domainKey, domainId) {
    return new Uint8Array(hkdfSync("sha256", domainKey, Buffer.from(domainId, "utf8"), Buffer.from(HKDF_INFO, "utf8"), AUTH_KEY_BYTES));
}
export function randomNonce() {
    return randomBytes(NONCE_BYTES);
}
export function randomToken(bytes = 32) {
    return randomBytes(bytes);
}
export function clientMac(domainAuthKey, domainId, clientNonce, brokerNonce, brokerEpoch) {
    const h = createHmac("sha256", domainAuthKey);
    h.update(CLIENT_LABEL);
    h.update(domainId);
    h.update(clientNonce);
    h.update(brokerNonce);
    h.update(brokerEpoch);
    return h.digest();
}
export function serverMac(domainAuthKey, domainId, clientNonce, brokerNonce, brokerEpoch) {
    const h = createHmac("sha256", domainAuthKey);
    h.update(SERVER_LABEL);
    h.update(domainId);
    h.update(clientNonce);
    h.update(brokerNonce);
    h.update(brokerEpoch);
    return h.digest();
}
export function macEqual(a, b) {
    if (a.length !== b.length)
        return false;
    return timingSafeEqual(Buffer.from(a), Buffer.from(b));
}
export function sha256Hex(value) {
    return createHash("sha256").update(value, "utf8").digest("hex");
}
export function base64url(data) {
    return Buffer.from(data).toString("base64url");
}
export function unbase64url(data, expectedBytes) {
    if (data.length === 0 || !/^[A-Za-z0-9_-]+$/.test(data))
        return null;
    try {
        const buf = Buffer.from(data, "base64url");
        if (buf.toString("base64url") !== data)
            return null;
        if (expectedBytes !== undefined && buf.length !== expectedBytes)
            return null;
        return new Uint8Array(buf);
    }
    catch {
        return null;
    }
}
