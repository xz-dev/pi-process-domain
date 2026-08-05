/**
 * Length-prefixed framing over a byte stream (node:net socket or named pipe).
 *
 *   uint32be payloadLength
 *   payloadLength bytes of UTF-8 canonical JSON (single object)
 *
 * Max frame 64 KiB; zero/oversized lengths are rejected before allocation.
 * The decoder is incremental and handles split/coalesced chunks.
 */
import { type CanonicalObject } from "./canonical.js";
export type { CanonicalObject };
export declare const MAX_FRAME_BYTES: number;
export declare const FRAME_HEADER_BYTES = 4;
export declare class FrameTooLargeError extends Error {
    readonly requestedLength: number;
    constructor(requestedLength: number);
}
export declare function encodeFrame(payload: CanonicalObject): Buffer;
/**
 * Incremental frame decoder. Feed arbitrary chunks; emits complete payload
 * objects. Throws FrameTooLargeError / Error for oversized or malformed input.
 */
export declare class FrameDecoder {
    private buffer;
    private onFrame;
    private onError;
    constructor(onFrame: (frame: CanonicalObject) => void, onError: (error: Error) => void);
    push(chunk: Buffer): void;
    private drain;
}
export declare function readFrameLength(buf: Buffer): number | null;
