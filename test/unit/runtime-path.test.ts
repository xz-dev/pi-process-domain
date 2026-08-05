import { describe, expect, it } from "vitest";
import { resolveEndpointFor } from "../../src/internal/runtime-path.js";

describe("runtime endpoint derivation (platform-independent)", () => {
  it("unix/macOS/FreeBSD without XDG uses a per-uid private tmpdir path", () => {
    const ep = resolveEndpointFor({
      platform: "linux",
      uid: 1000,
      username: "alice",
      tmpdir: "/tmp",
    });
    expect(ep.platform).toBe("unix");
    expect(ep.runtimeDir).toBe("/tmp/pi-process-domain/uid-1000");
    expect(ep.endpointPath).toBe("/tmp/pi-process-domain/uid-1000/broker.sock");
  });

  it("freebsd resolves the same shape as linux", () => {
    const ep = resolveEndpointFor({
      platform: "freebsd",
      uid: 1001,
      username: "bob",
      tmpdir: "/var/tmp",
    });
    expect(ep.platform).toBe("unix");
    expect(ep.runtimeDir).toBe("/var/tmp/pi-process-domain/uid-1001");
    expect(ep.endpointPath).toBe("/var/tmp/pi-process-domain/uid-1001/broker.sock");
  });

  it("darwin honors XDG_RUNTIME_DIR", () => {
    const ep = resolveEndpointFor({
      platform: "darwin",
      uid: 501,
      username: "carol",
      xdg: "/run/user/501",
      tmpdir: "/tmp",
    });
    expect(ep.platform).toBe("unix");
    expect(ep.runtimeDir).toBe("/run/user/501/pi-process-domain/v1");
    expect(ep.endpointPath).toBe("/run/user/501/pi-process-domain/v1/broker.sock");
  });

  it("windows resolves a deterministic named pipe keyed to the user hash", () => {
    const ep = resolveEndpointFor({
      platform: "win32",
      username: "dave",
      tmpdir: "C:\\Users\\dave\\AppData\\Local\\Temp",
    });
    expect(ep.platform).toBe("win32");
    expect(ep.runtimeDir).toBe("");
    // A stable hash of "user:dave" drives the pipe name; must be deterministic.
    const again = resolveEndpointFor({
      platform: "win32",
      username: "dave",
      tmpdir: "C:\\Users\\dave\\AppData\\Local\\Temp",
    });
    expect(ep.endpointPath).toBe(again.endpointPath);
    expect(ep.endpointPath.startsWith("\\\\.\\pipe\\pi-process-domain-v1-")).toBe(true);
    // Different users get different pipes.
    const other = resolveEndpointFor({
      platform: "win32",
      username: "erin",
      tmpdir: "C:\\Users\\erin\\AppData\\Local\\Temp",
    });
    expect(other.endpointPath).not.toBe(ep.endpointPath);
  });

  it("uses a hashed username when no uid is available (non-root fallback)", () => {
    const ep = resolveEndpointFor({
      platform: "linux",
      username: "fallback-user",
      tmpdir: "/tmp",
    });
    expect(ep.runtimeDir).toMatch(/^\/tmp\/pi-process-domain\/uid-[0-9a-f]{16}$/);
  });
});
