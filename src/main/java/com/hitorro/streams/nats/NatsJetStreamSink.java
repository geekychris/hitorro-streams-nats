/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.streams.nats;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.util.core.iterator.sinks.Sink;
import com.hitorro.util.io.StoreException;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.Nats;
import io.nats.client.Options;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * NATS JetStream sink — implements {@link Sink}{@code <JsonNode>}. Each
 * incoming row is serialized as JSON and published to a JetStream subject.
 *
 * <p>Two publish modes:</p>
 * <ul>
 *   <li><b>Sync (default)</b> — {@code publish()} blocks until the server
 *       acks the message. Safer for at-least-once, slower.</li>
 *   <li><b>Async</b> — {@code publishAsync()} returns a
 *       {@link CompletableFuture}; sink tracks in-flight futures and drains
 *       them at {@link #stop()}. Higher throughput, but a broker failure
 *       between publish and stop means some acks may be lost.</li>
 * </ul>
 *
 * <p>The subject can be static ({@link Builder#subject(String)}) or per-row
 * ({@link Builder#subjectExtractor(Function)}).</p>
 */
public final class NatsJetStreamSink implements Sink<JsonNode> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Connection conn;
    private final JetStream js;
    private final Function<JsonNode, String> subjectExtractor;
    private final boolean async;
    private final java.util.List<CompletableFuture<?>> inFlight;

    private NatsJetStreamSink(Builder b) throws Exception {
        Options opts = new Options.Builder()
                .server(b.url)
                .connectionTimeout(b.connectTimeout)
                .build();
        this.conn = Nats.connect(opts);
        this.js = conn.jetStream();
        this.subjectExtractor = b.subjectExtractor;
        this.async = b.async;
        this.inFlight = async ? new java.util.ArrayList<>() : null;
    }

    @Override public boolean init(JsonNode config) { return true; }
    @Override public boolean start() { return true; }

    @Override public boolean add(JsonNode row) throws IOException, StoreException {
        try {
            String subj = subjectExtractor.apply(row);
            byte[] payload = MAPPER.writeValueAsBytes(row);
            if (async) {
                inFlight.add(js.publishAsync(subj, payload));
            } else {
                js.publish(subj, payload);
            }
            return true;
        } catch (Exception e) {
            throw new IOException("NatsJetStreamSink.add failed: " + e.getMessage(), e);
        }
    }

    @Override public boolean stop() {
        if (async) {
            try {
                CompletableFuture.allOf(inFlight.toArray(new CompletableFuture[0]))
                        .get(Duration.ofSeconds(30).toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
                inFlight.clear();
            } catch (Exception e) {
                // In-flight acks lost — surface via logs at whatever caller wraps the sink.
            }
        }
        return true;
    }

    @Override public void close() throws IOException {
        try { conn.close(); } catch (Exception ignored) {}
    }

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private String url = Options.DEFAULT_URL;
        private String subject;
        private Function<JsonNode, String> subjectExtractor;
        private boolean async = false;
        private Duration connectTimeout = Duration.ofSeconds(5);

        public Builder url(String u) { this.url = u; return this; }
        /** Publish every row to this fixed subject. */
        public Builder subject(String s) {
            this.subject = s;
            this.subjectExtractor = row -> s;
            return this;
        }
        /** Compute the subject per row. Overrides any static subject. */
        public Builder subjectExtractor(Function<JsonNode, String> f) {
            this.subjectExtractor = f;
            return this;
        }
        public Builder async(boolean a) { this.async = a; return this; }
        public Builder connectTimeout(Duration d) { this.connectTimeout = d; return this; }
        public NatsJetStreamSink build() throws Exception {
            if (subjectExtractor == null) {
                throw new IllegalStateException("subject or subjectExtractor is required");
            }
            return new NatsJetStreamSink(this);
        }
    }
}
