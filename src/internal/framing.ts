/**
 * Length-prefixed framing over a byte stream (node:net socket or named pipe).
 *
 *   uint32be payloadLength
 *   payloadLength bytes of UTF-8 canonical JSON (single object)
 *
 * Max frame 64 KiB; zero/oversized lengths are rejected before allocation.
 * The decoder is incremental and handles split/coalesced chunks.
 */

import { parseCanonicalObject, canonicalSerialize, type CanonicalObject } from "./canonical.js";

export type { CanonicalObject };

export const MAX_FRAME_BYTES = 64 * 1024;
export const FRAME_HEADER_BYTES = 4;

export class FrameTooLargeError extends Error {
  readonly requestedLength: number;
  constructor(requestedLength: number) {
    super(`frame length ${requestedLength} exceeds maximum ${MAX_FRAME_BYTES}`);
    this.name = "FrameTooLargeError";
    this.requestedLength = requestedLength;
  }
}

export function encodeFrame(payload: CanonicalObject): Buffer {
  const body = Buffer.from(canonicalSerialize(payload), "utf8");
  if (body.length > MAX_FRAME_BYTES) {
    throw new FrameTooLargeError(body.length);
  }
  const buf = Buffer.allocUnsafe(FRAME_HEADER_BYTES + body.length);
  buf.writeUInt32BE(body.length, 0);
  body.copy(buf, FRAME_HEADER_BYTES);
  return buf;
}

/**
 * Incremental frame decoder. Feed arbitrary chunks; emits complete payload
 * objects. Throws FrameTooLargeError / Error for oversized or malformed input.
 */
export class FrameDecoder {
  private buffer: Buffer = Buffer.alloc(0);
  private onFrame: (frame: CanonicalObject) => void;
  private onError: (error: Error) => void;

  constructor(
    onFrame: (frame: CanonicalObject) => void,
    onError: (error: Error) => void,
  ) {
    this.onFrame = onFrame;
    this.onError = onError;
  }

  push(chunk: Buffer): void {
    this.buffer = this.buffer.length === 0 ? chunk : Buffer.concat([this.buffer, chunk]);
    this.drain();
  }

  private drain(): void {
    for (;;) {
      if (this.buffer.length < FRAME_HEADER_BYTES) return;
      const length = this.buffer.readUInt32BE(0);
      if (length === 0) {
        this.onError(new Error("frame length zero is invalid"));
        this.buffer = Buffer.alloc(0);
        return;
      }
      if (length > MAX_FRAME_BYTES) {
        this.onError(new FrameTooLargeError(length));
        this.buffer = Buffer.alloc(0);
        return;
      }
      if (this.buffer.length < FRAME_HEADER_BYTES + length) return;
      const body = this.buffer.subarray(FRAME_HEADER_BYTES, FRAME_HEADER_BYTES + length);
      this.buffer = this.buffer.subarray(FRAME_HEADER_BYTES + length);
      let payload: CanonicalObject;
      try {
        payload = parseCanonicalObject(body.toString("utf8"));
      }
      catch (error) {
        this.onError(error instanceof Error ? error : new Error("malformed frame"));
        this.buffer = Buffer.alloc(0);
        return;
      }
      this.onFrame(payload);
    }
  }
}

export function readFrameLength(buf: Buffer): number | null {
  if (buf.length < FRAME_HEADER_BYTES) return null;
  return buf.readUInt32BE(0);
}
