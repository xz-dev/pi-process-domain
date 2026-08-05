import { describe, expect, it } from "vitest";
import {
  FrameDecoder,
  FrameTooLargeError,
  MAX_FRAME_BYTES,
  encodeFrame,
} from "../../src/internal/framing.js";

describe("framing", () => {
  it("round-trips a single frame", () => {
    const frames: unknown[] = [];
    const decoder = new FrameDecoder(
      (f) => frames.push(f),
      () => {
        throw new Error("no error expected");
      },
    );
    const buf = encodeFrame({ hello: "world", n: 1 });
    decoder.push(buf);
    expect(frames).toEqual([{ hello: "world", n: 1 }]);
  });

  it("handles split and coalesced chunks incrementally", () => {
    const frames: unknown[] = [];
    const decoder = new FrameDecoder((f) => frames.push(f), () => {
      throw new Error("no error");
    });
    const buf = encodeFrame({ a: 1 });
    const buf2 = encodeFrame({ b: 2 });
    // split first frame byte-by-byte
    for (let i = 0; i < buf.length; i++) {
      decoder.push(buf.subarray(i, i + 1));
    }
    // coalesce second frame entirely
    decoder.push(buf2);
    expect(frames).toEqual([{ a: 1 }, { b: 2 }]);
  });

  it("rejects an oversized frame length without allocating", () => {
    const errors: Error[] = [];
    const decoder = new FrameDecoder(() => {
      throw new Error("no frame expected");
    }, (e) => errors.push(e));
    const header = Buffer.alloc(4);
    header.writeUInt32BE(MAX_FRAME_BYTES + 1, 0);
    decoder.push(header);
    expect(errors.length).toBe(1);
    expect(errors[0]).toBeInstanceOf(FrameTooLargeError);
  });

  it("rejects a zero frame length", () => {
    const errors: Error[] = [];
    const decoder = new FrameDecoder(() => {
      throw new Error("no frame expected");
    }, (e) => errors.push(e));
    decoder.push(Buffer.from([0, 0, 0, 0]));
    expect(errors.length).toBe(1);
    expect(errors[0]!.message).toMatch(/zero/i);
  });

  it("rejects a non-canonical payload", () => {
    const errors: Error[] = [];
    const decoder = new FrameDecoder(() => {
      throw new Error("no frame expected");
    }, (e) => errors.push(e));
    // length 9 => {"b":1,"a":2} (non-canonical key order)
    const body = Buffer.from('{"b":1,"a":2}');
    const buf = Buffer.concat([headerFor(body.length), body]);
    decoder.push(buf);
    expect(errors.length).toBe(1);
  });

  it("throws when encoding an over-max frame", () => {
    expect(() => encodeFrame({ data: "x".repeat(MAX_FRAME_BYTES) })).toThrow(FrameTooLargeError);
  });
});

function headerFor(length: number): Buffer {
  const h = Buffer.alloc(4);
  h.writeUInt32BE(length, 0);
  return h;
}
