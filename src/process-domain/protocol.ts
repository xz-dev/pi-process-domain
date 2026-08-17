import { createHmac, randomBytes, timingSafeEqual } from "node:crypto";
import type { PiLifecycleEvent, ProcessDomainDeclaration } from "./types.js";
import { PROCESS_DOMAIN_PROTOCOL } from "./types.js";

export const ENV_NAMES = {
  DECLARATION: "PI_EXTENSION_UTILS_PROCESS_DOMAIN",
} as const;

export const MAX_MESSAGE_BYTES = 64 * 1024;
export const MAX_CHANNEL_LENGTH = 128;
export const MAX_NODE_ID_LENGTH = 128;
const MAX_ENDPOINT_LENGTH = 2_048;
const MAX_METADATA_ENTRIES = 32;
const MAX_METADATA_KEY_LENGTH = 128;
const MAX_METADATA_VALUE_LENGTH = 1_024;

export type WireEnvelope =
  | {
      readonly version: 1;
      readonly type: "challenge-request";
      readonly phase: "bootstrap" | "hello";
      readonly domainId: string;
      readonly nodeId: string;
      readonly clientNonce: string;
    }
  | {
      readonly version: 1;
      readonly type: "challenge";
      readonly phase: "bootstrap" | "hello";
      readonly domainId: string;
      readonly nodeId: string;
      readonly clientNonce: string;
      readonly serverNonce: string;
    }
  | {
      readonly version: 1;
      readonly type: "bootstrap";
      readonly domainId: string;
      readonly nodeId: string;
      readonly metadata: Readonly<Record<string, string>>;
      readonly clientNonce: string;
      readonly serverNonce: string;
      readonly proof: string;
    }
  | {
      readonly version: 1;
      readonly type: "bootstrap-ready";
      readonly domainId: string;
      readonly nodeId: string;
      readonly endpoint: string;
      readonly clientNonce: string;
      readonly serverNonce: string;
      readonly proof: string;
    }
  | {
      readonly version: 1;
      readonly type: "hello";
      readonly domainId: string;
      readonly nodeId: string;
      readonly clientNonce: string;
      readonly serverNonce: string;
      readonly proof: string;
    }
  | {
      readonly version: 1;
      readonly type: "ready";
      readonly domainId: string;
      readonly nodeId: string;
      readonly clientNonce: string;
      readonly serverNonce: string;
      readonly proof: string;
    }
  | {
      readonly version: 1;
      readonly type: "data";
      readonly id: string;
      readonly channel: string;
      readonly value: unknown;
      readonly senderId: string;
      readonly targetId: string;
    }
  | {
      readonly version: 1;
      readonly type: "lifecycle";
      readonly id: string;
      readonly senderId: string;
      readonly event: PiLifecycleEvent;
    }
  | {
      readonly version: 1;
      readonly type: "ack";
      readonly id: string;
    };

const ID = /^[A-Za-z0-9_-]+$/;
const CHANNEL = /^[A-Za-z0-9_.:/-]+$/;
const METADATA_KEY = /^[A-Za-z0-9_.:/-]+$/;
const PROOF = /^[A-Za-z0-9_-]{43}$/;
const LIFECYCLE_NAMES = new Set([
  "session_start",
  "agent_start",
  "agent_end",
  "agent_settled",
  "turn_start",
  "turn_end",
  "session_shutdown",
]);

export function randomId(bytes = 16): string {
  return randomBytes(bytes).toString("base64url");
}

export function isValidId(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value.length > 0 &&
    value.length <= MAX_NODE_ID_LENGTH &&
    ID.test(value)
  );
}

export function isValidChannel(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value.length > 0 &&
    value.length <= MAX_CHANNEL_LENGTH &&
    CHANNEL.test(value)
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function hasExactKeys(value: Record<string, unknown>, keys: readonly string[]): boolean {
  const actual = Object.keys(value);
  return actual.length === keys.length && actual.every((key) => keys.includes(key));
}

function isValidProof(value: unknown): value is string {
  return typeof value === "string" && PROOF.test(value);
}

function isValidEndpoint(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value.length > 0 &&
    value.length <= MAX_ENDPOINT_LENGTH &&
    (value.startsWith("ipc://") || value.startsWith("tcp://127.0.0.1:"))
  );
}

function isValidMetadata(value: unknown): value is Readonly<Record<string, string>> {
  if (!isRecord(value)) return false;
  const entries = Object.entries(value);
  return (
    entries.length <= MAX_METADATA_ENTRIES &&
    entries.every(
      ([key, item]) =>
        key.length > 0 &&
        key.length <= MAX_METADATA_KEY_LENGTH &&
        METADATA_KEY.test(key) &&
        typeof item === "string" &&
        item.length <= MAX_METADATA_VALUE_LENGTH,
    )
  );
}

function snapshotJsonValue(value: unknown, ancestors = new WeakSet<object>()): unknown {
  if (value === null || typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value !== "object" || ancestors.has(value)) throw new TypeError("invalid JSON value");
  if (Object.getOwnPropertySymbols(value).length > 0) throw new TypeError("invalid JSON value");
  ancestors.add(value);
  try {
    const descriptors = Object.getOwnPropertyDescriptors(value);
    if (Array.isArray(value)) {
      const length = value.length;
      const keys = Object.keys(descriptors).filter((key) => key !== "length");
      if (keys.length !== length) throw new TypeError("invalid JSON value");
      const result: unknown[] = [];
      for (let index = 0; index < length; index += 1) {
        const descriptor = descriptors[String(index)];
        if (descriptor === undefined || descriptor.enumerable !== true || !Object.hasOwn(descriptor, "value")) {
          throw new TypeError("invalid JSON value");
        }
        result.push(snapshotJsonValue(descriptor.value, ancestors));
      }
      return result;
    }
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) throw new TypeError("invalid JSON value");
    const result = Object.create(null) as Record<string, unknown>;
    for (const [key, descriptor] of Object.entries(descriptors)) {
      if (descriptor.enumerable !== true || !Object.hasOwn(descriptor, "value")) {
        throw new TypeError("invalid JSON value");
      }
      result[key] = snapshotJsonValue(descriptor.value, ancestors);
    }
    return result;
  } finally {
    ancestors.delete(value);
  }
}

function isJsonValue(value: unknown): boolean {
  try {
    snapshotJsonValue(value);
    return true;
  } catch {
    return false;
  }
}

function isValidLifecycleEvent(value: unknown): value is PiLifecycleEvent {
  if (!isRecord(value)) return false;
  const allowed = ["name", "at", "sessionId", "details"] as const;
  if (Object.keys(value).some((key) => !allowed.includes(key as (typeof allowed)[number]))) return false;
  if (!LIFECYCLE_NAMES.has(String(value.name))) return false;
  if (!Number.isSafeInteger(value.at) || Number(value.at) < 0) return false;
  if (value.sessionId !== undefined && !isValidId(value.sessionId)) return false;
  return value.details === undefined || (isRecord(value.details) && isJsonValue(value.details));
}

function isValidEnvelope(value: unknown): value is WireEnvelope {
  if (!isRecord(value) || value.version !== PROCESS_DOMAIN_PROTOCOL || typeof value.type !== "string") return false;
  switch (value.type) {
    case "challenge-request":
      return (
        hasExactKeys(value, ["version", "type", "phase", "domainId", "nodeId", "clientNonce"]) &&
        (value.phase === "bootstrap" || value.phase === "hello") &&
        isValidId(value.domainId) &&
        isValidId(value.nodeId) &&
        isValidId(value.clientNonce)
      );
    case "challenge":
      return (
        hasExactKeys(value, ["version", "type", "phase", "domainId", "nodeId", "clientNonce", "serverNonce"]) &&
        (value.phase === "bootstrap" || value.phase === "hello") &&
        isValidId(value.domainId) &&
        isValidId(value.nodeId) &&
        isValidId(value.clientNonce) &&
        isValidId(value.serverNonce)
      );
    case "bootstrap":
      return (
        hasExactKeys(value, ["version", "type", "domainId", "nodeId", "metadata", "clientNonce", "serverNonce", "proof"]) &&
        isValidId(value.domainId) &&
        isValidId(value.nodeId) &&
        isValidMetadata(value.metadata) &&
        isValidId(value.clientNonce) &&
        isValidId(value.serverNonce) &&
        isValidProof(value.proof)
      );
    case "bootstrap-ready":
      return (
        hasExactKeys(value, ["version", "type", "domainId", "nodeId", "endpoint", "clientNonce", "serverNonce", "proof"]) &&
        isValidId(value.domainId) &&
        isValidId(value.nodeId) &&
        isValidEndpoint(value.endpoint) &&
        isValidId(value.clientNonce) &&
        isValidId(value.serverNonce) &&
        isValidProof(value.proof)
      );
    case "hello":
    case "ready":
      return (
        hasExactKeys(value, ["version", "type", "domainId", "nodeId", "clientNonce", "serverNonce", "proof"]) &&
        isValidId(value.domainId) &&
        isValidId(value.nodeId) &&
        isValidId(value.clientNonce) &&
        isValidId(value.serverNonce) &&
        isValidProof(value.proof)
      );
    case "data":
      return (
        hasExactKeys(value, ["version", "type", "id", "channel", "value", "senderId", "targetId"]) &&
        isValidId(value.id) &&
        isValidChannel(value.channel) &&
        isJsonValue(value.value) &&
        isValidId(value.senderId) &&
        (value.targetId === "*" || isValidId(value.targetId))
      );
    case "lifecycle":
      return (
        hasExactKeys(value, ["version", "type", "id", "senderId", "event"]) &&
        isValidId(value.id) &&
        isValidId(value.senderId) &&
        isValidLifecycleEvent(value.event)
      );
    case "ack":
      return hasExactKeys(value, ["version", "type", "id"]) && isValidId(value.id);
    default:
      return false;
  }
}

export function encodeEnvelope(envelope: WireEnvelope): Buffer {
  let snapshot: unknown;
  try {
    snapshot = snapshotJsonValue(envelope);
  } catch {
    throw new TypeError("invalid process-domain envelope");
  }
  if (!isValidEnvelope(snapshot)) throw new TypeError("invalid process-domain envelope");
  const encoded = Buffer.from(JSON.stringify(snapshot), "utf8");
  if (encoded.length > MAX_MESSAGE_BYTES) {
    throw new RangeError(`message exceeds ${MAX_MESSAGE_BYTES} bytes`);
  }
  return encoded;
}

export function decodeEnvelope(frame: Buffer): WireEnvelope {
  if (frame.length === 0 || frame.length > MAX_MESSAGE_BYTES) {
    throw new TypeError("invalid process-domain frame size");
  }
  let value: unknown;
  try {
    value = JSON.parse(frame.toString("utf8"));
  } catch {
    throw new TypeError("invalid process-domain JSON frame");
  }
  if (!isValidEnvelope(value)) throw new TypeError("invalid process-domain envelope");
  return value;
}

function isValidCapability(value: unknown): value is string {
  return isValidProof(value) && Buffer.from(value, "base64url").length === 32;
}

function isValidDeclaration(value: unknown): value is ProcessDomainDeclaration {
  return (
    isRecord(value) &&
    hasExactKeys(value, ["version", "domainId", "endpoint", "capability", "hostNodeId"]) &&
    value.version === PROCESS_DOMAIN_PROTOCOL &&
    isValidId(value.domainId) &&
    isValidId(value.hostNodeId) &&
    isValidCapability(value.capability) &&
    isValidEndpoint(value.endpoint)
  );
}

export function encodeDeclaration(declaration: ProcessDomainDeclaration): string {
  if (!isValidDeclaration(declaration)) throw new TypeError("process-domain declaration is invalid");
  return Buffer.from(JSON.stringify(declaration), "utf8").toString("base64url");
}

export function decodeDeclaration(value: string | undefined): ProcessDomainDeclaration | null {
  if (value === undefined) return null;
  let decoded: unknown;
  try {
    decoded = JSON.parse(Buffer.from(value, "base64url").toString("utf8"));
  } catch {
    throw new TypeError("process-domain declaration is malformed");
  }
  if (!isValidDeclaration(decoded)) throw new TypeError("process-domain declaration is invalid");
  return decoded;
}

export type ProofLabel = "bootstrap" | "bootstrap-ready" | "hello" | "ready";

export function createProof(
  capability: string,
  label: ProofLabel,
  values: readonly unknown[],
): string {
  return createHmac("sha256", Buffer.from(capability, "base64url"))
    .update(JSON.stringify([label, ...values]), "utf8")
    .digest("base64url");
}

export function verifyProof(
  capability: string,
  label: ProofLabel,
  values: readonly unknown[],
  proof: unknown,
): boolean {
  if (!isValidProof(proof)) return false;
  const expected = Buffer.from(createProof(capability, label, values), "base64url");
  const actual = Buffer.from(proof, "base64url");
  return actual.length === expected.length && timingSafeEqual(actual, expected);
}
