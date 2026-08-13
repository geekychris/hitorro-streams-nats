/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.streams.nats;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.jsontypesystem.JVS;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * NATS JetStream source — pull-subscribes to a subject on a JetStream stream
 * and exposes messages as an {@link Iterator}. Uses a durable pull consumer
 * so restarts pick up where the previous run left off.
 *
 * <p>Two shapes mirror {@link com.hitorro.streams.kafka.KafkaSource}:</p>
 * <ul>
 *   <li>{@link #asJvsIterator()} — parses each message's body as JSON and
 *       wraps in a {@link JVS}. Auto-acks after emission.</li>
 *   <li>{@link #asMessageIterator()} — raw {@link Message} handoff for callers
 *       that want manual ack, headers, metadata, or byte-level access.</li>
 * </ul>
 *
 * <p>Design notes:</p>
 * <ul>
 *   <li>Durable pull consumer — reuses the same consumer name across restarts
 *       so the server tracks acked offset for us.</li>
 *   <li>Batch fetches via {@code subscription.fetch(batchSize, timeout)} —
 *       small batches minimize per-message latency; large batches minimize RTT.</li>
 *   <li>Auto-ack in the JVS iterator; the raw iterator hands you the {@link Message}
 *       and you decide when to {@code msg.ack()}.</li>
 *   <li>Blocking on {@code hasNext()} — a fetch returning empty just refetches.
 *       Call {@link #close()} to drain and shut down the connection.</li>
 * </ul>
 */
public final class NatsJetStreamSource implements AutoCloseable {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Connection conn;
    private final JetStreamSubscription sub;
    private final int batchSize;
    private final Duration fetchTimeout;
    private final Deque<Message> buffer = new ArrayDeque<>();
    private volatile boolean closed;

    private NatsJetStreamSource(Builder b) throws Exception {
        Options options = new Options.Builder()
                .server(b.url)
                .connectionTimeout(b.connectTimeout)
                .build();
        this.conn = Nats.connect(options);
        JetStream js = conn.jetStream();

        ConsumerConfiguration.Builder cc = ConsumerConfiguration.builder();
        if (b.deliverPolicy != null) cc.deliverPolicy(b.deliverPolicy);
        if (b.ackWait != null) cc.ackWait(b.ackWait);

        PullSubscribeOptions.Builder opts = PullSubscribeOptions.builder()
                .stream(b.stream)
                .configuration(cc.build());
        if (b.durableName != null) opts.durable(b.durableName);

        this.sub = js.subscribe(b.subject, opts.build());
        this.batchSize = b.batchSize;
        this.fetchTimeout = b.fetchTimeout;
    }

    /** Iterator of raw NATS messages. Caller is responsible for {@code msg.ack()}. */
    public Iterator<Message> asMessageIterator() {
        return new Iterator<>() {
            @Override public boolean hasNext() {
                while (buffer.isEmpty() && !closed) {
                    List<Message> batch = sub.fetch(batchSize, fetchTimeout);
                    buffer.addAll(batch);
                }
                return !buffer.isEmpty();
            }
            @Override public Message next() {
                if (!hasNext()) throw new NoSuchElementException();
                return buffer.poll();
            }
        };
    }

    /**
     * Iterator of JVS documents parsed from each message body. Auto-acks
     * after successful parse+emit. Unparseable messages are nak'd and skipped
     * (so the server can redeliver or expire them per your consumer config).
     */
    public Iterator<JVS> asJvsIterator() {
        Iterator<Message> raw = asMessageIterator();
        return new Iterator<>() {
            private JVS next;
            @Override public boolean hasNext() {
                while (next == null && raw.hasNext()) {
                    Message m = raw.next();
                    try {
                        JsonNode node = MAPPER.readTree(m.getData());
                        next = new JVS(node);
                        m.ack();
                    } catch (Exception e) {
                        try { m.nak(); } catch (Exception ignored) {}
                    }
                }
                return next != null;
            }
            @Override public JVS next() {
                if (!hasNext()) throw new NoSuchElementException();
                JVS out = next; next = null; return out;
            }
        };
    }

    /** Unblock any in-flight fetch and close the underlying connection. Idempotent. */
    @Override public void close() {
        if (closed) return;
        closed = true;
        try { sub.unsubscribe(); } catch (Exception ignored) {}
        try { conn.close(); } catch (Exception ignored) {}
    }

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private String url = Options.DEFAULT_URL;
        private String stream;
        private String subject;
        private String durableName;
        private int batchSize = 100;
        private Duration fetchTimeout = Duration.ofSeconds(1);
        private Duration connectTimeout = Duration.ofSeconds(5);
        private Duration ackWait;
        private io.nats.client.api.DeliverPolicy deliverPolicy;

        public Builder url(String u) { this.url = u; return this; }
        public Builder stream(String s) { this.stream = s; return this; }
        public Builder subject(String s) { this.subject = s; return this; }
        public Builder durableName(String d) { this.durableName = d; return this; }
        public Builder batchSize(int n) { this.batchSize = n; return this; }
        public Builder fetchTimeout(Duration d) { this.fetchTimeout = d; return this; }
        public Builder connectTimeout(Duration d) { this.connectTimeout = d; return this; }
        public Builder ackWait(Duration d) { this.ackWait = d; return this; }
        public Builder deliverPolicy(io.nats.client.api.DeliverPolicy p) { this.deliverPolicy = p; return this; }

        public NatsJetStreamSource build() throws Exception {
            if (stream == null || subject == null) {
                throw new IllegalStateException("stream and subject are required");
            }
            return new NatsJetStreamSource(this);
        }
    }
}
