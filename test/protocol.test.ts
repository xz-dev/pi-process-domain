import { describe, expect, it } from "vitest";
import {
  createProof,
  decodeDeclaration,
  decodeEnvelope,
  encodeDeclaration,
  encodeEnvelope,
  randomId,
  type WireEnvelope,
} from "../src/process-domain/protocol.js";
import { PROCESS_DOMAIN_PROTOCOL } from "../src/process-domain/types.js";

function frame(value: unknown): Buffer {
  return Buffer.from(JSON.stringify(value), "utf8");
}

describe("process-domain protocol validation", () => {
  it("round-trips valid declarations and rejects ambiguous declarations", () => {
    const declaration = {
      version: PROCESS_DOMAIN_PROTOCOL,
      domainId: randomId(),
      endpoint: "tcp://127.0.0.1:12345",
      capability: randomId(32),
      hostNodeId: randomId(),
    } as const;
    expect(decodeDeclaration(encodeDeclaration(declaration))).toEqual(declaration);
    const invalid = { ...declaration, capability: declaration.capability, extra: true };
    expect(() => decodeDeclaration(Buffer.from(JSON.stringify(invalid)).toString("base64url"))).toThrow("invalid");
  });

  it("strictly validates each wire envelope and never carries capability material", () => {
    const capability = randomId(32);
    const valid: WireEnvelope = {
      version: PROCESS_DOMAIN_PROTOCOL,
      type: "hello",
      domainId: randomId(),
      nodeId: randomId(),
      clientNonce: randomId(),
      serverNonce: randomId(),
      proof: createProof(capability, "hello", ["bound"]),
    };
    const encoded = encodeEnvelope(valid);
    expect(decodeEnvelope(encoded)).toEqual(valid);
    expect(encoded.toString("utf8")).not.toContain(capability);

    expect(() => decodeEnvelope(frame({ ...valid, extra: true }))).toThrow("invalid process-domain envelope");
    expect(() => decodeEnvelope(frame({ ...valid, clientNonce: "bad nonce" }))).toThrow("invalid process-domain envelope");
    expect(() => decodeEnvelope(frame({ ...valid, proof: 1 }))).toThrow("invalid process-domain envelope");
    expect(() => decodeEnvelope(frame({ version: 1, type: "ack", id: "bad id" }))).toThrow("invalid process-domain envelope");
    expect(() => decodeEnvelope(frame({ version: 1, type: "data", id: randomId(), channel: "bad channel!", value: null, senderId: randomId(), targetId: "*" }))).toThrow("invalid process-domain envelope");
    expect(() => decodeEnvelope(frame({ version: 1, type: "lifecycle", id: randomId(), senderId: randomId(), event: { name: "unknown", at: 1 } }))).toThrow("invalid process-domain envelope");
  });

  it("rejects values that cannot be preserved as strict JSON", () => {
    const data = (value: unknown): WireEnvelope => ({
      version: PROCESS_DOMAIN_PROTOCOL,
      type: "data",
      id: randomId(),
      channel: "strict-json",
      value,
      senderId: randomId(),
      targetId: "*",
    });
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    expect(() => encodeEnvelope(data(undefined))).toThrow("invalid process-domain envelope");
    expect(() => encodeEnvelope(data(Number.NaN))).toThrow("invalid process-domain envelope");
    expect(() => encodeEnvelope(data(Number.POSITIVE_INFINITY))).toThrow("invalid process-domain envelope");
    expect(() => encodeEnvelope(data(1n))).toThrow("invalid process-domain envelope");
    expect(() => encodeEnvelope(data(cyclic))).toThrow("invalid process-domain envelope");
    expect(() => encodeEnvelope(data({ toJSON: () => "changed" }))).toThrow("invalid process-domain envelope");
    const hiddenToJson = { original: true };
    Object.defineProperty(hiddenToJson, "toJSON", { enumerable: false, value: () => ({ changed: true }) });
    expect(() => encodeEnvelope(data(hiddenToJson))).toThrow("invalid process-domain envelope");
    const accessor = Object.defineProperty({}, "value", { enumerable: true, get: () => "changed" });
    expect(() => encodeEnvelope(data(accessor))).toThrow("invalid process-domain envelope");
    const sparse = new Array(1);
    expect(() => encodeEnvelope(data(sparse))).toThrow("invalid process-domain envelope");
    const shared = { ok: true };
    expect(() => encodeEnvelope(data({ first: shared, second: shared }))).not.toThrow();
  });
});
