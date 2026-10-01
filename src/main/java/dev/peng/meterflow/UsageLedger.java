package dev.peng.meterflow;

import dev.peng.meterflow.Contracts.ReservationView;
import dev.peng.meterflow.Contracts.UsageInput;
import dev.peng.meterflow.Contracts.UsageView;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

/** Applies a batch of one tenant's balance operations under a single row lock and a single commit. */
@Component
class UsageLedger {
    private final JdbcTemplate jdbc;
    private final Clock clock;

    UsageLedger(JdbcTemplate jdbc, Clock clock) {
        this.jdbc = jdbc;
        this.clock = clock;
    }

    /** A request that changes a tenant's balance. Every kind goes through the same batched path. */
    sealed interface Operation permits Charge, Reserve, Commit, Release {
        String keyHash();
        String requestId();
    }

    /** Usage reported after the fact. */
    record Charge(String keyHash, UsageInput input) implements Operation {
        public String requestId() { return input.requestId(); }
    }

    /** Holds quota before an upstream call; {@code units} is the most the call may consume. */
    record Reserve(String keyHash, String requestId, String model, long units, Duration ttl) implements Operation {}

    /** Records the actual usage of a reservation and returns the unused part to the tenant. */
    record Commit(String keyHash, String requestId, long units) implements Operation {}

    /** Returns a reservation's quota when the upstream call did not happen. */
    record Release(String keyHash, String requestId) implements Operation {}

    /** Exactly one of {@code view} ({@link UsageView} or {@link ReservationView}) and {@code error} is set. */
    record Outcome(Object view, ApiError error) {
        static Outcome ok(Object view) { return new Outcome(view, null); }
        static Outcome rejected(ApiError error) { return new Outcome(null, error); }
    }

    /**
     * Operations are decided in order, as if each ran in its own transaction after the previous one.
     * A per-operation rejection does not affect the rest; a database failure rolls back the whole batch.
     */
    @Transactional(isolation = Isolation.READ_COMMITTED)
    List<Outcome> apply(String tenantId, List<? extends Operation> operations) {
        // All writes for one tenant serialize on this row; everything below is read after the lock.
        Account account = lockAccount(tenantId);
        if (account == null) {
            return operations.stream().map(o -> Outcome.rejected(invalidKey())).toList();
        }
        Set<String> keyHashes = new LinkedHashSet<>();
        Set<String> requestIds = new LinkedHashSet<>();
        operations.forEach(o -> {
            keyHashes.add(o.keyHash());
            requestIds.add(o.requestId());
        });
        // Revocation also takes the tenant lock, so a key revoked before this point is seen here.
        Set<String> activeKeys = activeKeys(tenantId, keyHashes);
        Batch batch = new Batch(tenantId, account, existingEvents(tenantId, requestIds),
                existingReservations(tenantId, requestIds), clock.instant());
        List<Outcome> outcomes = new ArrayList<>(operations.size());
        for (Operation operation : operations) {
            outcomes.add(activeKeys.contains(operation.keyHash()) ? batch.decide(operation)
                    : Outcome.rejected(invalidKey()));
        }
        batch.write();
        return outcomes;
    }

    /** Expires one tenant's overdue reservations; returns how many were expired. */
    @Transactional(isolation = Isolation.READ_COMMITTED)
    int expireOverdue(String tenantId) {
        Instant now = clock.instant();
        if (lockAccount(tenantId) == null) {
            return 0;
        }
        List<Object[]> overdue = jdbc.query("SELECT id, reserved_units FROM usage_reservations "
                        + "WHERE tenant_id = ? AND state = 'RESERVED' AND expires_at <= ?",
                (rs, row) -> new Object[] {rs.getString("id"), rs.getLong("reserved_units")},
                tenantId, Timestamp.from(now));
        if (overdue.isEmpty()) {
            return 0;
        }
        Timestamp settled = Timestamp.from(now);
        jdbc.batchUpdate("UPDATE usage_reservations SET state = 'EXPIRED', settled_at = ? WHERE id = ?",
                overdue.stream().map(r -> new Object[] {settled, r[0]}).toList());
        long freed = overdue.stream().mapToLong(r -> (Long) r[1]).sum();
        jdbc.update("UPDATE tenants SET reserved_units = reserved_units - ? WHERE id = ?", freed, tenantId);
        return overdue.size();
    }

    private Account lockAccount(String tenantId) {
        return jdbc.query("SELECT quota_units, used_units, reserved_units FROM tenants WHERE id = ? FOR UPDATE",
                (rs, row) -> new Account(rs.getLong("quota_units"), rs.getLong("used_units"),
                        rs.getLong("reserved_units")), tenantId).stream().findFirst().orElse(null);
    }

    private Set<String> activeKeys(String tenantId, Set<String> hashes) {
        return new HashSet<>(jdbc.queryForList("SELECT secret_hash FROM api_keys WHERE tenant_id = ? "
                        + "AND revoked = FALSE AND secret_hash IN (" + placeholders(hashes.size()) + ")",
                String.class, prepend(tenantId, hashes)));
    }

    private Map<String, Existing> existingEvents(String tenantId, Set<String> requestIds) {
        Map<String, Existing> found = new HashMap<>();
        jdbc.query("SELECT id, request_id, model_name, units FROM usage_events WHERE tenant_id = ? "
                        + "AND request_id IN (" + placeholders(requestIds.size()) + ")",
                rs -> {
                    found.put(rs.getString("request_id"), new Existing(rs.getString("id"),
                            rs.getString("model_name"), rs.getLong("units")));
                }, prepend(tenantId, requestIds));
        return found;
    }

    private Map<String, Reservation> existingReservations(String tenantId, Set<String> requestIds) {
        Map<String, Reservation> found = new HashMap<>();
        jdbc.query("SELECT id, request_id, model_name, reserved_units, state, committed_units, event_id, expires_at "
                        + "FROM usage_reservations WHERE tenant_id = ? AND request_id IN ("
                        + placeholders(requestIds.size()) + ")",
                rs -> {
                    Reservation r = new Reservation(rs.getString("id"), rs.getString("request_id"),
                            rs.getString("model_name"), rs.getLong("reserved_units"),
                            rs.getTimestamp("expires_at").toInstant(), false);
                    r.state = State.valueOf(rs.getString("state"));
                    long committed = rs.getLong("committed_units");
                    r.committedUnits = rs.wasNull() ? null : committed;
                    r.eventId = rs.getString("event_id");
                    found.put(r.requestId, r);
                }, prepend(tenantId, requestIds));
        return found;
    }

    /** In-memory account for one batch; {@link #write()} persists the net effect. */
    private final class Batch {
        final String tenantId;
        final Account before;
        final Map<String, Existing> events;
        final Map<String, Reservation> reservations;
        final Instant now;
        long used;
        long reserved;
        final List<Object> eventArgs = new ArrayList<>();

        Batch(String tenantId, Account before, Map<String, Existing> events,
              Map<String, Reservation> reservations, Instant now) {
            this.tenantId = tenantId;
            this.before = before;
            this.events = events;
            this.reservations = reservations;
            this.now = now;
            this.used = before.used();
            this.reserved = before.reserved();
        }

        long available() {
            return before.quota() - used - reserved;
        }

        Outcome decide(Operation operation) {
            if (operation instanceof Charge charge) return charge(charge.input());
            if (operation instanceof Reserve reserve) return reserve(reserve);
            if (operation instanceof Commit commit) return commit(commit);
            return release((Release) operation);
        }

        Outcome charge(UsageInput input) {
            if (reservations.containsKey(input.requestId())) {
                return Outcome.rejected(conflict("该 requestId 已用于额度预留"));
            }
            Existing old = events.get(input.requestId());
            if (old != null) {
                if (old.units() != input.units() || !old.model().equals(input.model())) {
                    return Outcome.rejected(conflict("相同 requestId 已记录不同的用量"));
                }
                return Outcome.ok(new UsageView(old.id(), tenantId, input.requestId(), input.model(),
                        old.units(), true, available()));
            }
            if (input.units() > available()) {
                return Outcome.rejected(quotaExceeded());
            }
            String eventId = addEvent(input.requestId(), input.model(), input.units());
            used += input.units();
            return Outcome.ok(new UsageView(eventId, tenantId, input.requestId(), input.model(),
                    input.units(), false, available()));
        }

        Outcome reserve(Reserve request) {
            Reservation old = reservations.get(request.requestId());
            if (old != null) {
                if (old.units != request.units() || !old.model.equals(request.model())) {
                    return Outcome.rejected(conflict("相同 requestId 已预留不同的用量"));
                }
                return Outcome.ok(view(old, true));
            }
            if (events.containsKey(request.requestId())) {
                return Outcome.rejected(conflict("该 requestId 已直接上报过用量"));
            }
            if (request.units() > available()) {
                return Outcome.rejected(quotaExceeded());
            }
            Reservation created = new Reservation(UUID.randomUUID().toString(), request.requestId(),
                    request.model(), request.units(), now.plus(request.ttl()), true);
            created.state = State.RESERVED;
            reservations.put(created.requestId, created);
            reserved += created.units;
            return Outcome.ok(view(created, false));
        }

        Outcome commit(Commit request) {
            Reservation r = reservations.get(request.requestId());
            if (r == null) {
                return Outcome.rejected(notFound());
            }
            switch (r.state) {
                case COMMITTED:
                    return r.committedUnits == request.units() ? Outcome.ok(view(r, true))
                            : Outcome.rejected(conflict("预留已按不同用量结算"));
                case RELEASED:
                    return Outcome.rejected(closed("RESERVATION_RELEASED", "预留已释放，不能结算"));
                case EXPIRED:
                    return Outcome.rejected(closed("RESERVATION_EXPIRED", "预留已过期，不能结算"));
                default:
                    break;
            }
            if (!now.isBefore(r.expiresAt)) {
                expire(r);
                return Outcome.rejected(closed("RESERVATION_EXPIRED", "预留已过期，不能结算"));
            }
            if (request.units() > r.units) {
                // The reservation is the ceiling the caller agreed to; it stays open for a correct commit.
                return Outcome.rejected(new ApiError(HttpStatus.UNPROCESSABLE_ENTITY, "COMMIT_EXCEEDS_RESERVATION",
                        "实际用量超过预留额度"));
            }
            reserved -= r.units;
            used += request.units();
            r.eventId = addEvent(r.requestId, r.model, request.units());
            r.committedUnits = request.units();
            settle(r, State.COMMITTED);
            return Outcome.ok(view(r, false));
        }

        Outcome release(Release request) {
            Reservation r = reservations.get(request.requestId());
            if (r == null) {
                return Outcome.rejected(notFound());
            }
            switch (r.state) {
                case COMMITTED:
                    return Outcome.rejected(closed("RESERVATION_COMMITTED", "预留已结算，不能释放"));
                case RELEASED:
                case EXPIRED:
                    return Outcome.ok(view(r, true));
                default:
                    break;
            }
            if (!now.isBefore(r.expiresAt)) {
                expire(r);
            } else {
                reserved -= r.units;
                settle(r, State.RELEASED);
            }
            return Outcome.ok(view(r, false));
        }

        void expire(Reservation r) {
            reserved -= r.units;
            settle(r, State.EXPIRED);
        }

        void settle(Reservation r, State state) {
            r.state = state;
            r.settledAt = now;
            r.dirty = true;
        }

        String addEvent(String requestId, String model, long units) {
            String eventId = UUID.randomUUID().toString();
            eventArgs.addAll(List.of(eventId, tenantId, requestId, model, units, Timestamp.from(now)));
            // A later operation in this batch with the same requestId sees this event.
            events.put(requestId, new Existing(eventId, model, units));
            return eventId;
        }

        ReservationView view(Reservation r, boolean replayed) {
            return new ReservationView(r.requestId, tenantId, r.model, r.units, r.state.name(), r.committedUnits,
                    r.eventId, r.expiresAt, replayed, available());
        }

        void write() {
            if (!eventArgs.isEmpty()) {
                insertRows("usage_events(id, tenant_id, request_id, model_name, units, created_at)", 6, eventArgs);
            }
            Collection<Reservation> all = reservations.values();
            List<Object> created = new ArrayList<>();
            List<Object[]> settled = new ArrayList<>();
            for (Reservation r : all) {
                if (r.isNew) {
                    created.addAll(Arrays.asList(r.id, tenantId, r.requestId, r.model, r.units,
                            r.state.name(), r.committedUnits, r.eventId, Timestamp.from(r.expiresAt),
                            Timestamp.from(now), r.settledAt == null ? null : Timestamp.from(r.settledAt)));
                } else if (r.dirty) {
                    settled.add(new Object[] {r.state.name(), r.committedUnits, r.eventId,
                            Timestamp.from(r.settledAt), r.id});
                }
            }
            if (!created.isEmpty()) {
                insertRows("usage_reservations(id, tenant_id, request_id, model_name, reserved_units, state, "
                        + "committed_units, event_id, expires_at, created_at, settled_at)", 11, created);
            }
            if (!settled.isEmpty()) {
                jdbc.batchUpdate("UPDATE usage_reservations SET state = ?, committed_units = ?, event_id = ?, "
                        + "settled_at = ? WHERE id = ?", settled);
            }
            long usedDelta = used - before.used();
            long reservedDelta = reserved - before.reserved();
            if (usedDelta != 0 || reservedDelta != 0) {
                jdbc.update("UPDATE tenants SET used_units = used_units + ?, reserved_units = reserved_units + ? "
                        + "WHERE id = ?", usedDelta, reservedDelta, tenantId);
            }
        }

        private void insertRows(String table, int columns, List<Object> args) {
            String row = "(" + placeholders(columns) + ")";
            jdbc.update("INSERT INTO " + table + " VALUES "
                    + String.join(", ", Collections.nCopies(args.size() / columns, row)), args.toArray());
        }
    }

    private static String placeholders(int count) {
        return String.join(", ", Collections.nCopies(count, "?"));
    }

    private static Object[] prepend(String first, Set<String> rest) {
        List<Object> args = new ArrayList<>(rest.size() + 1);
        args.add(first);
        args.addAll(rest);
        return args.toArray();
    }

    private static ApiError invalidKey() {
        return new ApiError(HttpStatus.UNAUTHORIZED, "INVALID_API_KEY", "API 密钥无效");
    }

    private static ApiError conflict(String message) {
        return new ApiError(HttpStatus.CONFLICT, "IDEMPOTENCY_CONFLICT", message);
    }

    private static ApiError quotaExceeded() {
        return new ApiError(HttpStatus.TOO_MANY_REQUESTS, "QUOTA_EXCEEDED", "租户剩余配额不足");
    }

    private static ApiError notFound() {
        return new ApiError(HttpStatus.NOT_FOUND, "RESERVATION_NOT_FOUND", "预留不存在");
    }

    private static ApiError closed(String code, String message) {
        return new ApiError(HttpStatus.CONFLICT, code, message);
    }

    private enum State { RESERVED, COMMITTED, RELEASED, EXPIRED }

    private record Account(long quota, long used, long reserved) {}
    private record Existing(String id, String model, long units) {}

    private static final class Reservation {
        final String id;
        final String requestId;
        final String model;
        final long units;
        final Instant expiresAt;
        final boolean isNew;
        State state;
        Long committedUnits;
        String eventId;
        Instant settledAt;
        boolean dirty;

        Reservation(String id, String requestId, String model, long units, Instant expiresAt, boolean isNew) {
            this.id = id;
            this.requestId = requestId;
            this.model = model;
            this.units = units;
            this.expiresAt = expiresAt;
            this.isNew = isNew;
        }
    }
}
