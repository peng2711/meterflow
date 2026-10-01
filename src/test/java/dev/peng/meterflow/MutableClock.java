package dev.peng.meterflow;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicReference;

/** A clock tests can move forward, so expiry is checked without sleeping. */
final class MutableClock extends Clock {
    private final AtomicReference<Instant> now = new AtomicReference<>(Instant.now());

    void advance(Duration duration) {
        now.updateAndGet(t -> t.plus(duration));
    }

    @Override public ZoneId getZone() { return ZoneOffset.UTC; }
    @Override public Clock withZone(ZoneId zone) { throw new UnsupportedOperationException(); }
    @Override public Instant instant() { return now.get(); }
}
