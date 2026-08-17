/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.util.core.iterator.sinks.nats;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.util.core.iterator.sinks.JsonNodeSinkBase;
import io.nats.client.Connection;
import io.nats.client.Nats;

import java.io.IOException;
import java.time.Duration;

/**
 * Fire-and-forget NATS core-publish sink. Every row is serialised to
 * JSON bytes and published to {@code subject}. Rows land on the NATS
 * message bus; other pipelines can consume them via {@code NatsSource}
 * to form a multi-process compute graph without a shared file store.
 *
 * <p>Lives in {@code hitorro-streams-nats} — any Sink caller with NATS
 * on the classpath can use it. No mesh dependency.</p>
 */
public final class NatsSink extends JsonNodeSinkBase {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final String servers;
    private final String subject;
    private Connection conn;

    public NatsSink(String servers, String subject) {
        this.servers = servers == null ? "nats://localhost:4222" : servers;
        this.subject = subject;
    }

    public String servers() { return servers; }
    public String subject() { return subject; }

    @Override
    public boolean start() throws IOException {
        try {
            conn = Nats.connect(servers);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("NatsSink connect interrupted", e);
        }
    }

    @Override
    protected void writeRow(JsonNode row) throws IOException {
        if (conn == null) start();
        conn.publish(subject, JSON.writeValueAsBytes(row));
        // Flush per message so tests using small volumes see them arrive
        // immediately at subscribers. High-throughput jobs can rely on
        // jnats's own batching plus close()-time flush.
        try { conn.flush(Duration.ofSeconds(2)); }
        catch (Exception e) { throw new IOException("NatsSink flush failed", e); }
    }

    @Override
    public void close() throws IOException {
        try { if (conn != null) conn.flush(Duration.ofSeconds(2)); } catch (Exception ignored) { }
        try { if (conn != null) conn.close(); } catch (Exception ignored) { }
        conn = null;
    }
}
