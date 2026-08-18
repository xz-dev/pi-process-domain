import { EventEmitter } from "node:events";
import type net from "node:net";
import { afterEach, describe, expect, it, vi } from "vitest";
import { wrapSocket } from "../src/process-domain/net.js";

class StalledSocket extends EventEmitter {
  destroyed = false;
  destroyCalls = 0;

  setNoDelay(): this {
    return this;
  }

  write(_packet: Buffer, _callback: (error?: Error | null) => void): boolean {
    return true;
  }

  destroy(): this {
    this.destroyCalls += 1;
    this.destroyed = true;
    this.emit("close");
    return this;
  }
}

describe("process-domain framed link", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("forces close and rejects a stalled write after the bounded drain", async () => {
    vi.useFakeTimers();
    const socket = new StalledSocket();
    const link = wrapSocket(socket as unknown as net.Socket);

    const writeFailure = link.send(Buffer.from("{}")).then(
      () => null,
      (error: unknown) => error,
    );
    link.close();
    expect(socket.destroyCalls).toBe(0);

    vi.advanceTimersByTime(99);
    expect(socket.destroyCalls).toBe(0);
    vi.advanceTimersByTime(1);
    expect(socket.destroyCalls).toBe(1);
    expect(link.closed).toBe(true);
    await expect(writeFailure).resolves.toEqual(expect.objectContaining({ message: "process-domain connection closed" }));
  });
});
