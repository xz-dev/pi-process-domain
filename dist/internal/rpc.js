/**
 * Internal RPC/transport layer.
 *
 * A socket (Unix domain socket or Windows named pipe) carries length-prefixed
 * canonical-JSON frames. The handshake is performed at the raw frame level;
 * once authenticated, the same socket is upgraded to a birpc channel for
 * request/response dispatch. birpc is hidden behind this adapter and never part
 * of the public interface.
 */
import { createBirpc } from "../vendor/birpc/main.js";
import { FrameDecoder, encodeFrame } from "./framing.js";
import { ProcessDomainFatalError, isProcessDomainFatalError, } from "./errors.js";
const ERROR_MARKER = "__piProcessDomainError";
function serializeRpc(value) {
    if (value instanceof ProcessDomainFatalError) {
        return { [ERROR_MARKER]: true, code: value.code, message: value.message };
    }
    if (value instanceof Error) {
        return { [ERROR_MARKER]: true, code: "BROKER_UNAVAILABLE", message: value.message };
    }
    if (Array.isArray(value))
        return value.map(serializeRpc);
    if (value !== null && typeof value === "object") {
        const out = {};
        for (const [key, item] of Object.entries(value))
            out[key] = serializeRpc(item);
        return out;
    }
    return value;
}
function deserializeRpc(value) {
    if (Array.isArray(value))
        return value.map(deserializeRpc);
    if (value !== null && typeof value === "object") {
        const obj = value;
        if (obj[ERROR_MARKER] === true && typeof obj.code === "string" && typeof obj.message === "string") {
            return new ProcessDomainFatalError(obj.code, obj.message);
        }
        const out = {};
        for (const [key, item] of Object.entries(obj))
            out[key] = deserializeRpc(item);
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
    handlers = new Set();
    destroyedFlag = false;
    socket;
    onCloseCallback = () => { };
    closeDelivered = false;
    pendingBytes = 0;
    maxPendingBytes = 256 * 1024;
    constructor(socket, onProtocolError, onClose) {
        this.socket = socket;
        if (onClose)
            this.onCloseCallback = onClose;
        const decoder = new FrameDecoder((frame) => {
            if (this.destroyedFlag)
                return;
            const copy = Array.from(this.handlers);
            for (const fn of copy) {
                try {
                    fn(frame);
                }
                catch {
                    /* handler errors are isolated */
                }
            }
        }, (error) => {
            this.destroyedFlag = true;
            onProtocolError(error);
            socket.destroy();
        });
        socket.on("data", (chunk) => decoder.push(chunk));
        socket.on("end", () => this.handleClose());
        socket.on("error", (error) => this.handleClose(error));
        socket.on("close", () => this.handleClose());
    }
    addHandler(fn) {
        this.handlers.add(fn);
    }
    removeHandler(fn) {
        this.handlers.delete(fn);
    }
    onClose(fn) {
        this.onCloseCallback = fn;
    }
    send(obj) {
        if (this.destroyedFlag || this.socket.destroyed)
            return false;
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
    destroy(error) {
        if (!this.destroyedFlag) {
            this.destroyedFlag = true;
            this.socket.destroy();
        }
        this.deliverClose(error);
    }
    get destroyed() {
        return this.destroyedFlag;
    }
    handleClose(error) {
        this.destroyedFlag = true;
        this.deliverClose(error);
    }
    deliverClose(error) {
        if (this.closeDelivered)
            return;
        this.closeDelivered = true;
        this.onCloseCallback(error);
    }
}
/**
 * Create a birpc peer over an already-authenticated raw channel.
 * birpc's message handler is registered as a frame handler on the channel.
 */
export function createRpcPeer(raw, localFunctions) {
    let closed = false;
    let registeredHandler = null;
    const rpc = createBirpc(localFunctions, {
        post: (data) => {
            const ok = raw.send(data);
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
            if (registeredHandler)
                raw.removeHandler(registeredHandler);
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
            if (!isProcessDomainFatalError(error) && !raw.destroyed)
                raw.destroy(error);
            return false;
        },
        proxify: true,
    });
    return {
        rpc,
        close(error) {
            if (closed)
                return;
            closed = true;
            rpc.$close(error);
            raw.destroy(error);
        },
        get closed() {
            return closed;
        },
    };
}
