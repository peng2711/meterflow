package dev.peng.meterflow;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.sql.Timestamp;
import java.time.Clock;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Returns the quota of reservations whose holder never committed or released them, for example
 * because the caller crashed. Expiry itself is decided by {@code expires_at}: a late commit is
 * rejected whether or not this job has run yet.
 */
@Component
class ReservationSweeper {
    private static final Logger log = LoggerFactory.getLogger(ReservationSweeper.class);
    private static final int TENANTS_PER_RUN = 100;

    private final JdbcTemplate jdbc;
    private final UsageLedger ledger;
    private final Clock clock;
    private final Counter expired;

    ReservationSweeper(JdbcTemplate jdbc, UsageLedger ledger, Clock clock, MeterRegistry meters) {
        this.jdbc = jdbc;
        this.ledger = ledger;
        this.clock = clock;
        this.expired = Counter.builder("meterflow.usage.reservations.expired")
                .description("Reservations expired by the background sweep")
                .register(meters);
    }

    @Scheduled(fixedDelayString = "${meterflow.usage.reservation.sweep-interval}",
            initialDelayString = "${meterflow.usage.reservation.sweep-interval}")
    void scheduledSweep() {
        try {
            sweep();
        } catch (RuntimeException e) {
            log.warn("预留过期回收失败，将在下次调度重试", e);
        }
    }

    /** Expires overdue reservations of up to {@value #TENANTS_PER_RUN} tenants; returns how many. */
    int sweep() {
        List<String> tenantIds = jdbc.queryForList("SELECT DISTINCT tenant_id FROM usage_reservations "
                + "WHERE state = 'RESERVED' AND expires_at <= ? LIMIT " + TENANTS_PER_RUN,
                String.class, Timestamp.from(clock.instant()));
        int total = 0;
        for (String tenantId : tenantIds) {
            // Each tenant in its own transaction, so one hot tenant's lock does not hold up the rest.
            total += ledger.expireOverdue(tenantId);
        }
        expired.increment(total);
        return total;
    }
}
