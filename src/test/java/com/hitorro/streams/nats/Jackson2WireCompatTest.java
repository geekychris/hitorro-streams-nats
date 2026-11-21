/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.streams.nats;

import com.hitorro.jsontypesystem.JVS;
import com.hitorro.util.json.JsonMappers;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the NATS wire format to what 3.0.2 sent and accepted on Jackson 2: the message body both
 * sinks publish for parsed and code-built rows (number subtypes, escapes, binary, POJOs, rows that
 * cannot be written), and what {@link NatsJetStreamSource} makes of 36 message bodies (valid,
 * trailing content, malformed, deep nesting, empty, UTF-16/BOM/invalid UTF-8/Latin-1 bytes).
 *
 * <p>{@code jackson2-nats-wire.txt} was produced by running the same cases against the 3.0.2
 * classes on Jackson 2.18, through those classes' own mappers.</p>
 */
class Jackson2WireCompatTest {
  static final String[] ENCODERS = {"NatsJetStreamSink", "util.NatsSink"};
  static final String[] DECODERS = {"NatsJetStreamSource"};
  static final String[] KEYERS = {};
  static final String UTIL = "com.hitorro.util.core.iterator.sinks.nats.NatsSink";

  static byte[] utilEncode(tools.jackson.databind.JsonNode n) throws Throwable {
    Method m = Class.forName(UTIL).getDeclaredMethod("encode", tools.jackson.databind.JsonNode.class);
    m.setAccessible(true);
    try { return (byte[]) m.invoke(null, n); } catch (InvocationTargetException e) { throw e.getCause(); }
  }
  static tools.jackson.databind.JsonNode parse(String s) { return JsonMappers.json().readTree(s); }
  static String encode(String enc, tools.jackson.databind.JsonNode n) throws Throwable {
    byte[] b = enc.equals("NatsJetStreamSink") ? NatsJetStreamSink.encode(n) : utilEncode(n);
    return new String(b, StandardCharsets.UTF_8);
  }
  static String decodeBytes(byte[] body) {
    JVS j = NatsJetStreamSource.decode(body);
    return j == null ? "skip" : sig(j.getJsonNode());
  }
  static String decode(String dec, String value) { return decodeBytes(value.getBytes(StandardCharsets.UTF_8)); }
  static String key(String ke, tools.jackson.databind.JsonNode row, String path) { throw new UnsupportedOperationException(); }
  /**
   * Deliberate differences from Jackson 2 (everything else must match the golden file exactly):
   * <ul>
   * <li>java.time and Optional are built into Jackson 3. A row holding them in a POJO node could not
   * be sent by 3.x (IOException); it is now sent, the Instant as decimal epoch seconds (what Jackson 2
   * wrote with JavaTimeModule). Same decision as hitorro-util {@code Http}.</li>
   * </ul>
   */
  static final Map<String, String> ACCEPTED = Map.of(
      "NatsJetStreamSink.built.pojo.instant", "{\"p\":1700000000.123000000}",
      "util.NatsSink.built.pojo.instant", "{\"p\":1700000000.123000000}",
      "NatsJetStreamSink.built.pojo.optional", "{\"p\":\"v\"}",
      "util.NatsSink.built.pojo.optional", "{\"p\":\"v\"}");
  // ---- shared by the Jackson 2 generator and the Jackson 3 test (tools.jackson = jackson package) ----
  public record Rec(int b, String a, java.util.Date d, java.util.List<String> l) {}
  public static final class Bean { private int z = 1; private String y = "y"; public int getZ() { return z; } public String getY() { return y; } }

  static String nest(int depth) {
    StringBuilder b = new StringBuilder();
    for (int i = 0; i < depth; i++) b.append("[");
    for (int i = 0; i < depth; i++) b.append("]");
    return b.toString();
  }

  static final String[] TEXT_ROWS = {
    "{\"b\":1,\"a\":\"x\",\"n\":null,\"arr\":[1,2.5,\"s\",true,null],\"o\":{\"z\":1,\"y\":[]},\"big\":12345678901234567890,\"dec\":1.10,\"neg\":-0.0,\"l\":-9223372036854775808}",
    "{\"s\":\"\\u0000\\u001f\\\"\\\\/ \\u00e9 \\ud83d\\ude00 \\u2028 <&>'\"}",
    "{\"e\":1e400,\"f\":1.0E-5,\"g\":123456789.123456789,\"h\":0.1}",
    "[1,{\"a\":[]},\"x\"]",
    "\"just text\"",
    "42",
    "null",
    "{}"
  };

  static final String[] DECODE_INPUTS = {
    "{\"a\":1,\"b\":[true,null,\"x\"],\"c\":{\"d\":1.10}}",
    "{\"a\":1}{\"b\":2}", "{\"a\":1} trailing", "{\"a\":1}\n", "  {\"a\":1}  ",
    "", "   ", "nul", "null", "[1,2]", "\"s\"", "12345678901234567890", "1.10", "-0", "1e400",
    "{\"a\":1,}", "{'a':1}", "{a:1}", "{\"a\":NaN}", "{\"a\":01}", "/* c */ {\"a\":1}", "{\"a\":\"\u0001\"}",
    "\uFEFF{\"a\":1}", "{\"a\":1,\"a\":2}", "{\"a\":", "[1,2", "{\"a\":\"\\ud83d\\ude00\"}",
    nest(400), nest(600), nest(1200)
  };

  static final String[] KEY_PATHS = {"id", "n", "d", "b", "nul", "o", "a", "o.k", "o.k.x", "missing", "o.missing.x", "big", "neg"};
  static final String KEY_ROW = "{\"id\":\"doc-1\",\"n\":42,\"d\":1.10,\"b\":true,\"nul\":null,\"o\":{\"k\":\"v\",\"n\":[1]},\"a\":[1,2],\"big\":12345678901234567890,\"neg\":-0.0}";

  /** Rows that are built in code rather than parsed (number node subtypes, binary, POJOs). */
  static java.util.LinkedHashMap<String, tools.jackson.databind.JsonNode> builtRows() {
    tools.jackson.databind.node.JsonNodeFactory f = tools.jackson.databind.node.JsonNodeFactory.instance;
    java.util.LinkedHashMap<String, tools.jackson.databind.JsonNode> m = new java.util.LinkedHashMap<>();
    tools.jackson.databind.node.ObjectNode o = f.objectNode();
    o.put("bd110", new java.math.BigDecimal("1.10"));
    o.put("bd1e3", new java.math.BigDecimal("1E+3"));
    o.put("bdbig", new java.math.BigDecimal("12345678901234567890.000"));
    o.put("bi", new java.math.BigInteger("123456789012345678901234567890"));
    o.put("fl", 0.1f);
    o.put("dbl", 0.1d);
    o.put("nan", Double.NaN);
    o.put("inf", Double.NEGATIVE_INFINITY);
    o.put("lmax", Long.MAX_VALUE);
    o.put("sh", (short) 7);
    o.put("bin", new byte[]{0, 1, 2, (byte) 255, 64});
    o.put("bool", false);
    o.putNull("nul");
    o.set("arr", f.arrayNode().add(1).add("two").add(f.nullNode()).add(f.numberNode(new java.math.BigDecimal("0.00"))));
    m.put("numbers", o);
    java.util.LinkedHashMap<String, Object> pm = new java.util.LinkedHashMap<>();
    pm.put("zeta", 1); pm.put("alpha", java.util.List.of("x", 2)); pm.put("date", new java.util.Date(1700000000123L));
    m.put("pojo.map", f.objectNode().set("p", f.pojoNode(pm)));
    m.put("pojo.record", f.objectNode().set("p", f.pojoNode(new Rec(2, "a", new java.util.Date(5L), java.util.List.of("q")))));
    m.put("pojo.bean", f.objectNode().set("p", f.pojoNode(new Bean())));
    m.put("pojo.empty", f.objectNode().set("p", f.pojoNode(new Object())));
    m.put("pojo.instant", f.objectNode().set("p", f.pojoNode(java.time.Instant.ofEpochMilli(1700000000123L))));
    m.put("pojo.optional", f.objectNode().set("p", f.pojoNode(java.util.Optional.of("v"))));
    m.put("missing", tools.jackson.databind.node.MissingNode.getInstance());
    m.put("nullnode", f.nullNode());
    return m;
  }

  static String sig(tools.jackson.databind.JsonNode n) {
    if (n == null) return "<java-null>";
    if (n.isObject()) {
      StringBuilder b = new StringBuilder("{");
      for (java.util.Map.Entry<String, tools.jackson.databind.JsonNode> e : n.properties()) {
        if (b.length() > 1) b.append(',');
        b.append(e.getKey()).append(':').append(sig(e.getValue()));
      }
      return b.append('}').toString();
    }
    if (n.isArray()) {
      StringBuilder b = new StringBuilder("[");
      for (tools.jackson.databind.JsonNode c : n) { if (b.length() > 1) b.append(','); b.append(sig(c)); }
      return b.append(']').toString();
    }
    if (n.isNumber()) return n.numberType() + "(" + n.toString() + ")";
    return n.getNodeType() + "(" + n.toString() + ")";
  }

  static String err(Throwable t) {
    return t instanceof java.io.IOException ? "!IOException" : "!" + t.getClass().getSimpleName();
  }

  static String esc(String s) {
    if (s == null) return "<null>";
    if (s.length() > 300) return "len=" + s.length() + ":" + Integer.toHexString(s.hashCode());
    return s.replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r");
  }
  /** Message bodies that are not plain UTF-8 text. */
  static java.util.LinkedHashMap<String, byte[]> rawBodies() {
    java.util.LinkedHashMap<String, byte[]> m = new java.util.LinkedHashMap<>();
    m.put("utf16be", "{\"a\":\"é\"}".getBytes(java.nio.charset.StandardCharsets.UTF_16BE));
    m.put("utf16le.bom", "﻿{\"a\":1}".getBytes(java.nio.charset.StandardCharsets.UTF_16LE));
    m.put("utf8.bom", new byte[]{(byte) 0xEF, (byte) 0xBB, (byte) 0xBF, '{', '"', 'a', '"', ':', '1', '}'});
    m.put("invalid.utf8", new byte[]{'{', '"', 'a', '"', ':', '"', (byte) 0xC3, (byte) 0x28, '"', '}'});
    m.put("latin1", "{\"a\":\"café\"}".getBytes(java.nio.charset.StandardCharsets.ISO_8859_1));
    m.put("empty", new byte[0]);
    return m;
  }

  static String generateRaw() {
    StringBuilder out = new StringBuilder();
    for (java.util.Map.Entry<String, byte[]> e : rawBodies().entrySet()) {
      String v;
      try { v = esc(decodeBytes(e.getValue())); } catch (Throwable t) { v = err(t); }
      out.append("NatsJetStreamSource.raw.").append(e.getKey()).append('=').append(v).append('\n');
    }
    return out.toString();
  }

  static String generate() throws Exception {
    StringBuilder out = new StringBuilder();
    for (int i = 0; i < TEXT_ROWS.length; i++) {
      tools.jackson.databind.JsonNode n = parse(TEXT_ROWS[i]);
      for (String enc : ENCODERS) {
        String v;
        try { v = esc(encode(enc, n)); } catch (Throwable t) { v = err(t); }
        out.append(enc).append(".text").append(i).append('=').append(v).append('\n');
      }
    }
    for (java.util.Map.Entry<String, tools.jackson.databind.JsonNode> e : builtRows().entrySet()) {
      for (String enc : ENCODERS) {
        String v;
        try { v = esc(encode(enc, e.getValue())); } catch (Throwable t) { v = err(t); }
        out.append(enc).append(".built.").append(e.getKey()).append('=').append(v).append('\n');
      }
    }
    for (int i = 0; i < DECODE_INPUTS.length; i++) {
      for (String dec : DECODERS) {
        String v;
        try { v = esc(decode(dec, DECODE_INPUTS[i])); } catch (Throwable t) { v = err(t); }
        out.append(dec).append(".in").append(i).append('=').append(v).append('\n');
      }
    }
    tools.jackson.databind.JsonNode kr = parse(KEY_ROW);
    for (String k : KEY_PATHS) {
      for (String ke : KEYERS) {
        String v;
        try { v = esc(key(ke, kr, k)); } catch (Throwable t) { v = err(t); }
        out.append(ke).append(".key.").append(k).append('=').append(v).append('\n');
      }
    }
    return out.toString();
  }

  @Test
  void wireFormatMatchesJackson2() throws Throwable {
    Map<String, String> golden = new LinkedHashMap<>();
    try (InputStream in = getClass().getResourceAsStream("jackson2-nats-wire.txt")) {
      for (String line : new String(in.readAllBytes(), StandardCharsets.UTF_8).split("\n")) {
        int eq = line.indexOf('=');
        if (eq > 0) golden.put(line.substring(0, eq), line.substring(eq + 1));
      }
    }
    Map<String, String> actual = new LinkedHashMap<>();
    for (String line : (generate() + generateRaw()).split("\n")) {
      int eq = line.indexOf('=');
      if (eq > 0) actual.put(line.substring(0, eq), line.substring(eq + 1));
    }
    List<String> diffs = new ArrayList<>();
    for (String k : golden.keySet()) {
      String expected = ACCEPTED.containsKey(k) ? ACCEPTED.get(k) : golden.get(k);
      if (!Objects.equals(expected, actual.get(k))) diffs.add(k + "\n  expected: " + expected + (ACCEPTED.containsKey(k) ? " (accepted difference; jackson2: " + golden.get(k) + ")" : "") + "\n  now:      " + actual.get(k));
    }
    for (String k : ACCEPTED.keySet()) assertTrue(golden.containsKey(k), "accepted difference for unknown key " + k);
    for (String k : actual.keySet()) if (!golden.containsKey(k)) diffs.add(k + " (not in golden): " + actual.get(k));
    assertTrue(diffs.isEmpty(), diffs.size() + " differences:\n" + String.join("\n", diffs));
    assertEquals(70, golden.size());
  }
}
