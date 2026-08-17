import { describe, expect, it } from "vitest";
import { buildXmlDocument, neutralizeAssistantMessage, parseTrailingXml } from "../src/xml.js";

describe("strict trailing XML", () => {
  it("builds escaped fields and parses one trailing document", () => {
    const xml = buildXmlDocument("reflection", [
      { name: "type", value: "NO_ISSUE" },
      { name: "reason", value: "A < B & C" },
    ]);
    const parsed = parseTrailingXml(`Checked.\n${xml}`, "reflection");
    expect(parsed.valid).toBe(true);
    if (parsed.valid) {
      expect(parsed.value.fields.get("type")).toBe("NO_ISSUE");
      expect(parsed.value.fields.get("reason")).toBe("A < B & C");
    }
  });

  it("rejects duplicate roots, fields, attributes, and trailing prose", () => {
    expect(parseTrailingXml("<x><a>1</a></x><x><a>2</a></x>", "x").valid).toBe(false);
    expect(parseTrailingXml("<x><a>1</a><a>2</a></x>", "x").valid).toBe(false);
    expect(parseTrailingXml("<x id=\"1\"><a>1</a></x>", "x").valid).toBe(false);
    expect(parseTrailingXml("<x><a>1</a></x> later", "x").valid).toBe(false);
  });

  it("treats XML names as case-sensitive", () => {
    expect(parseTrailingXml("<X><a>1</a></X>", "x").valid).toBe(false);
    expect(parseTrailingXml("<x><A>1</a></x>", "x").valid).toBe(false);
  });

  it("uses only XML whitespace and preserves field text", () => {
    const parsed = parseTrailingXml("<x>\n<a>\u00a0v\u00a0</a>\t</x>\r\n", "x");
    expect(parsed.valid).toBe(true);
    if (parsed.valid) expect(parsed.value.fields.get("a")).toBe("\u00a0v\u00a0");
    expect(parseTrailingXml("<x>\u00a0<a>v</a></x>", "x").valid).toBe(false);
    expect(parseTrailingXml("<x><a>v</a></x>\u00a0", "x").valid).toBe(false);
  });

  it("rejects invalid names and XML 1.0 characters", () => {
    expect(() => buildXmlDocument("x", [{ name: "bad name", value: "1" }])).toThrow("invalid XML field name");
    expect(() => buildXmlDocument("x", [{ name: "a><evil", value: "1" }])).toThrow("invalid XML field name");
    expect(() => buildXmlDocument("x", [{ name: "a", value: "bad\u0001" }])).toThrow("invalid XML text character");
    expect(parseTrailingXml("<x><a>bad\u0001</a></x>", "x").valid).toBe(false);
    expect(parseTrailingXml("<x><a>&#0;</a></x>", "x").valid).toBe(false);
    expect(parseTrailingXml("<x><a>&#x110000;</a></x>", "x").valid).toBe(false);
    expect(parseTrailingXml("<x><a>&unknown;</a></x>", "x").valid).toBe(false);
    expect(parseTrailingXml("<x><a>&amp</a></x>", "x").valid).toBe(false);
  });

  it("neutralizes assistant content without mutating the input", () => {
    const message = { role: "assistant", content: [{ type: "text", text: "secret" }], stopReason: "stop" };
    expect(neutralizeAssistantMessage(message)).toEqual({ ...message, content: [] });
    expect(message.content).toHaveLength(1);
  });
});
