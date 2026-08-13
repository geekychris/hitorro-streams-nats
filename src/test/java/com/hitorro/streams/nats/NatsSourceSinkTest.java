/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.streams.nats;

import com.hitorro.util.core.iterator.sinks.Sink;
import io.nats.client.Message;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Smoke tests. Full end-to-end tests would need a running NATS server
 * (Testcontainers has a NATS image) — too heavy for default unit runs.
 * These verify the builder API contract and Sink type compatibility.
 */
class NatsSourceSinkTest {

    @Test
    void sourceBuilder_requiresStreamAndSubject() {
        assertThatThrownIs(() -> NatsJetStreamSource.builder()
                .url("nats://localhost:4222")
                .build(),
            IllegalStateException.class,
            "stream and subject are required");
    }

    @Test
    void sinkBuilder_requiresSubject() {
        assertThatThrownIs(() -> NatsJetStreamSink.builder().build(),
            IllegalStateException.class,
            "subject or subjectExtractor is required");
    }

    @Test
    void sinkImplementsSinkOfJsonNode() {
        assertThat(Sink.class.isAssignableFrom(NatsJetStreamSink.class)).isTrue();
    }

    @Test
    void messageShape_matchesJnats() {
        Class<?> msgCls = Message.class;
        assertThat(msgCls.getName()).isEqualTo("io.nats.client.Message");
    }

    // -- helper ---------------------------------------------------------------

    /** Runnable that may throw a checked exception. */
    @FunctionalInterface
    private interface ThrowingRunnable { void run() throws Throwable; }

    private static void assertThatThrownIs(ThrowingRunnable r, Class<? extends Throwable> t, String msgFragment) {
        try { r.run(); }
        catch (Throwable actual) {
            assertThat(actual).isInstanceOf(t);
            assertThat(actual.getMessage()).contains(msgFragment);
            return;
        }
        throw new AssertionError("expected " + t.getSimpleName() + " containing '" + msgFragment + "'");
    }
}
