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

  it("fresh domains derive distinct bounded Unix endpoints", () => {
    const input = {
      platform: "linux" as const,
      uid: 1000,
      username: "alice",
      xdg: "/run/user/1000",
      tmpdir: "/tmp",
    };
    const a = resolveEndpointFor(input, { protocolMajor: 2, domainId: "domain-a-123456" });
    const same = resolveEndpointFor(input, { protocolMajor: 2, domainId: "domain-a-123456" });
    const b = resolveEndpointFor(input, { protocolMajor: 2, domainId: "domain-b-123456" });
    expect(a).toEqual(same);
    expect(a.endpointPath).not.toBe(b.endpointPath);
    expect(a.endpointPath).toMatch(/\/pi-process-domain\/v1\/d-[0-9a-f]{24}\.sock$/);
    expect(a.endpointPath).not.toContain("domain-a-123456");
  });

  it("uses a deterministic private short path when the complete Unix endpoint would be too long", () => {
    const longXdg = `/tmp/${"long-runtime-component/".repeat(8)}`;
    const input = {
      platform: "linux" as const,
      uid: 1000,
      username: "alice",
      xdg: longXdg,
      tmpdir: "/tmp",
    };
    const a = resolveEndpointFor(input, { protocolMajor: 2, domainId: "domain-a-123456" });
    const same = resolveEndpointFor(input, { protocolMajor: 2, domainId: "domain-a-123456" });
    const otherBase = resolveEndpointFor({ ...input, xdg: `${longXdg}other` }, {
      protocolMajor: 2,
      domainId: "domain-a-123456",
    });

    expect(a).toEqual(same);
    expect(a.runtimeDir).toMatch(/^\/tmp\/pi-pd-[0-9a-f]{24}$/);
    expect(a.endpointPath.length).toBeLessThanOrEqual(100);
    expect(a.endpointPath).not.toContain(longXdg);
    expect(a.endpointPath).not.toContain("domain-a-123456");
    expect(a.endpointPath).not.toBe(otherBase.endpointPath);
  });

  it("keeps rejecting relative Unix runtime bases instead of falling back around validation", () => {
    const ep = resolveEndpointFor({
      platform: "linux",
      uid: 1000,
      username: "alice",
      xdg: "relative/".repeat(20),
      tmpdir: "/tmp",
    }, { protocolMajor: 2, domainId: "domain-a-123456" });

    expect(ep.runtimeDir).toMatch(/^relative\//);
  });

  it("windows resolves a deterministic named pipe keyed to user and fresh domain", () => {
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
    const domain = resolveEndpointFor({
      platform: "win32",
      username: "dave",
      tmpdir: "C:\\Users\\dave\\AppData\\Local\\Temp",
    }, { protocolMajor: 2, domainId: "domain-a-123456" });
    const otherDomain = resolveEndpointFor({
      platform: "win32",
      username: "dave",
      tmpdir: "C:\\Users\\dave\\AppData\\Local\\Temp",
    }, { protocolMajor: 2, domainId: "domain-b-123456" });
    expect(domain.endpointPath).toMatch(/^\\\\\.\\pipe\\pi-process-domain-v2-[0-9a-f]{16}-[0-9a-f]{24}$/);
    expect(domain.endpointPath).not.toBe(otherDomain.endpointPath);
    expect(domain.endpointPath.length).toBeLessThan(100);
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
