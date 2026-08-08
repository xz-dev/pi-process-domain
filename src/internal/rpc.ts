/**
 * Internal RPC/transport layer.
 *
 * A socket (Unix domain socket or Windows named pipe) carries length-prefixed
 * canonical-JSON frames. The handshake is performed at the raw frame level;
 * once authenticated, the same socket is upgraded to a birpc channel for
 * request/response dispatch. birpc is hidden behind this adapter and never part
 * of the public interface.
 */

import type { Socket } from "node:net";
import { createBirpc, type BirpcReturn } from "../vendor/birpc/main.js";
import { FrameDecoder, encodeFrame, type CanonicalObject } from "./framing.js";
import {
  ProcessDomainFatalError,
  isProcessDomainFatalError,
  type ProcessDomainFatalCode,
} from "./errors.js";

export type LocalFunctions = Record<string, (...args: any[]) => unknown>;
export type RemoteFunctions = Record<string, (...args: any[]) => unknown>;
export type FrameHandler = (frame: CanonicalObject) => void;

const ERROR_MARKER = "__piProcessDomainError";

function serializeRpc(value: unknown): unknown {
  if (value instanceof ProcessDomainFatalError) {
    return { [ERROR_MARKER]: true, code: value.code, message: value.message };
  }
  if (value instanceof Error) {
    return { [ERROR_MARKER]: true, code: "BROKER_UNAVAILABLE", message: value.message };
  }
  if (Array.isArray(value)) return value.map(serializeRpc);
  if (value !== null && typeof value === "object") {
    const out: Record<string, unknown> = {};
    for (const [key, item] of Object.entries(value)) out[key] = serializeRpc(item);
    return out;
  }
  return value;
}

function deserializeRpc(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(deserializeRpc);
  if (value !== null && typeof value === "object") {
    const obj = value as Record<string, unknown>;
    if (obj[ERROR_MARKER] === true && typeof obj.code === "string" && typeof obj.message === "string") {
      return new ProcessDomainFatalError(obj.code as ProcessDomainFatalCode, obj.message);
    }
    const out: Record<string, unknown> = {};
    for (const [key, item] of Object.entries(obj)) out[key] = deserializeRpc(item);
    return out;
  }
  return value;
}

/**
 * Raw framed channel over a socket. Any number of frame handlers may register;
 * each decoded frame is dispatched to all current handlers. Used for the
 * handshake exchange, then a birpc message handler is registered on upgrade.
 */
export class RawChannel {
  private handlers = new Set<FrameHandler>();
  private destroyedFlag = false;
  private socket: Socket;
  private onCloseCallback: (error?: Error) => void = () => {};
  private closeDelivered = false;
  private pendingBytes = 0;
  private readonly maxPendingBytes = 256 * 1024;

  constructor(
    socket: Socket,
    onProtocolError: (error: Error) => void,
    onClose?: (error?: Error) => void,
  ) {
    this.socket = socket;
    if (onClose) this.onCloseCallback = onClose;
    const decoder = new FrameDecoder(
      (frame) => {
        if (this.destroyedFlag) return;
        const copy = Array.from(this.handlers);
        for (const fn of copy) {
          try {
            fn(frame);
          }
          catch {
            /* handler errors are isolated */
          }
        }
      },
      (error) => {
        this.destroyedFlag = true;
        onProtocolError(error);
        socket.destroy();
      },
    );
    socket.on("data", (chunk) => decoder.push(chunk));
    socket.on("end", () => this.handleClose());
    socket.on("error", (error) => this.handleClose(error));
    socket.on("close", () => this.handleClose());
  }

  addHandler(fn: FrameHandler): void {
    this.handlers.add(fn);
  }

  removeHandler(fn: FrameHandler): void {
    this.handlers.delete(fn);
  }

  onClose(fn: (error?: Error) => void): void {
    this.onCloseCallback = fn;
  }

  send(obj: CanonicalObject): boolean {
    if (this.destroyedFlag || this.socket.destroyed) return false;
    try {
      const frame = encodeFrame(obj);
      if (this.pendingBytes + frame.length > this.maxPendingBytes) {
        this.destroy(new Error("outbound queue limit exceeded"));
        return false;
      }
      this.pendingBytes += frame.length;
      this.socket.write(frame, () => {
        this.pendingBytes = Math.max(0, this.pendingBytes - frame.length);
      });
      return true;
    }
    catch (error) {
      this.destroyedFlag = true;
      this.socket.destroy();
      this.deliverClose(error instanceof Error ? error : new Error("transport send failed"));
      return false;
    }
  }

  destroy(error?: Error): void {
    if (!this.destroyedFlag) {
      this.destroyedFlag = true;
      this.socket.destroy();
    }
    this.deliverClose(error);
  }

  get destroyed(): boolean {
    return this.destroyedFlag;
  }

  private handleClose(error?: Error): void {
    this.destroyedFlag = true;
    this.deliverClose(error);
  }

  private deliverClose(error?: Error): void {
    if (this.closeDelivered) return;
    this.closeDelivered = true;
    this.onCloseCallback(error);
  }
}

export interface RpcPeer<
  R extends RemoteFunctions = RemoteFunctions,
  L extends LocalFunctions = LocalFunctions,
> {
  rpc: BirpcReturn<R, L, true>;
  close(error?: Error): void;
  readonly closed: boolean;
}

/**
 * Create a birpc peer over an already-authenticated raw channel.
 * birpc's message handler is registered as a frame handler on the channel.
 */
export function createRpcPeer<
  R extends RemoteFunctions,
  L extends LocalFunctions,
>(
  raw: RawChannel,
  localFunctions: L,
): RpcPeer<R, L> {
  let closed = false;
  let registeredHandler: FrameHandler | null = null;
  const rpc = createBirpc<R, L>(localFunctions, {
    post: (data) => {
      const ok = raw.send(data as CanonicalObject);
      if (!ok) {
        // Transport send failed (socket destroyed / protocol error). Reject the
        // pending call so it never hangs silently.
        throw new Error("[rpc] transport send failed");
      }
    },
    on: (fn) => {
      registeredHandler = (frame) => fn(frame);
      raw.addHandler(registeredHandler);
    },
    off: () => {
      if (registeredHandler) raw.removeHandler(registeredHandler);
      registeredHandler = null;
    },
    timeout: 15000,
    serialize: serializeRpc,
    deserialize: deserializeRpc,
    onGeneralError: (error) => {
      // Remote typed failures are part of the authenticated protocol and must
      // reach the caller intact. Transport/general failures close the channel
      // so an established client can reconnect, but also reject the operation;
      // callers must never receive `undefined` as a false success result.
      if (!isProcessDomainFatalError(error) && !raw.destroyed) raw.destroy(error);
      return false;
    },
    proxify: true,
  });

  return {
    rpc,
    close(error) {
      if (closed) return;
      closed = true;
      rpc.$close(error);
      raw.destroy(error);
    },
    get closed() {
      return closed;
    },
  };
}
