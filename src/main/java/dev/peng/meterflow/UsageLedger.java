package dev.peng.meterflow;

import dev.peng.meterflow.Contracts.UsageInput;
import dev.peng.meterflow.Contracts.UsageView;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
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

/** Applies a batch of one tenant's charges under a single row lock and a single commit. */
@Component
class UsageLedger {
    private final JdbcTemplate jdbc;

    UsageLedger(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    record Charge(String keyHash, UsageInput input) {}

    /** Exactly one of {@code view} and {@code error} is set. */
    record Outcome(UsageView view, ApiError error) {
        static Outcome ok(UsageView view) { return new Outcome(view, null); }
        static Outcome rejected(ApiError error) { return new Outcome(null, error); }
    }

    /**
     * Charges are decided in order, as if each ran in its own transaction after the previous one.
     * A per-charge rejection does not affect the rest; a database failure rolls back the whole batch.
     */
    @Transactional(isolation = Isolation.READ_COMMITTED)
    List<Outcome> apply(String tenantId, List<Charge> charges) {
        // All writes for one tenant serialize on this row; everything below is read after the lock.
        Account account = jdbc.query("SELECT quota_units, used_units FROM tenants WHERE id = ? FOR UPDATE",
                (rs, row) -> new Account(rs.getLong("quota_units"), rs.getLong("used_units")), tenantId)
                .stream().findFirst().orElse(null);
        if (account == null) {
            return charges.stream().map(c -> Outcome.rejected(invalidKey())).toList();
        }
        // Revocation also takes the tenant lock, so a key revoked before this point is seen here.
        Set<String> activeKeys = activeKeys(tenantId, charges);
        Map<String, Existing> recorded = existingEvents(tenantId, charges);

        long remaining = account.quota() - account.used();
        long charged = 0;
        Timestamp now = Timestamp.from(Instant.now());
        List<Object> insertArgs = new ArrayList<>();
        List<Outcome> outcomes = new ArrayList<>(charges.size());
        for (Charge charge : charges) {
            UsageInput input = charge.input();
            if (!activeKeys.contains(charge.keyHash())) {
                outcomes.add(Outcome.rejected(invalidKey()));
                continue;
            }
            Existing old = recorded.get(input.requestId());
            if (old != null) {
                if (old.units() != input.units() || !old.model().equals(input.model())) {
                    outcomes.add(Outcome.rejected(new ApiError(HttpStatus.CONFLICT, "IDEMPOTENCY_CONFLICT",
                            "相同 requestId 已记录不同的用量")));
                } else {
                    outcomes.add(Outcome.ok(new UsageView(old.id(), tenantId, input.requestId(), input.model(),
                            old.units(), true, remaining)));
                }
                continue;
            }
            if (input.units() > remaining) {
                outcomes.add(Outcome.rejected(new ApiError(HttpStatus.TOO_MANY_REQUESTS, "QUOTA_EXCEEDED",
                        "租户剩余配额不足")));
                continue;
            }
            String eventId = UUID.randomUUID().toString();
            insertArgs.addAll(List.of(eventId, tenantId, input.requestId(), input.model(), input.units(), now));
            // A later charge in this batch with the same requestId replays this one.
            recorded.put(input.requestId(), new Existing(eventId, input.model(), input.units()));
            remaining -= input.units();
            charged += input.units();
            outcomes.add(Outcome.ok(new UsageView(eventId, tenantId, input.requestId(), input.model(),
                    input.units(), false, remaining)));
        }
        if (charged > 0) {
            int rows = insertArgs.size() / 6;
            jdbc.update("INSERT INTO usage_events(id, tenant_id, request_id, model_name, units, created_at) VALUES "
                    + String.join(", ", Collections.nCopies(rows, "(?, ?, ?, ?, ?, ?)")), insertArgs.toArray());
            jdbc.update("UPDATE tenants SET used_units = used_units + ? WHERE id = ?", charged, tenantId);
        }
        return outcomes;
    }

    private Set<String> activeKeys(String tenantId, List<Charge> charges) {
        Set<String> hashes = new LinkedHashSet<>();
        charges.forEach(c -> hashes.add(c.keyHash()));
        return new HashSet<>(jdbc.queryForList("SELECT secret_hash FROM api_keys WHERE tenant_id = ? "
                        + "AND revoked = FALSE AND secret_hash IN (" + placeholders(hashes.size()) + ")",
                String.class, prepend(tenantId, hashes)));
    }

    private Map<String, Existing> existingEvents(String tenantId, List<Charge> charges) {
        Set<String> requestIds = new LinkedHashSet<>();
        charges.forEach(c -> requestIds.add(c.input().requestId()));
        Map<String, Existing> found = new HashMap<>();
        jdbc.query("SELECT id, request_id, model_name, units FROM usage_events WHERE tenant_id = ? "
                        + "AND request_id IN (" + placeholders(requestIds.size()) + ")",
                rs -> {
                    found.put(rs.getString("request_id"), new Existing(rs.getString("id"),
                            rs.getString("model_name"), rs.getLong("units")));
                }, prepend(tenantId, requestIds));
        return found;
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

    private record Account(long quota, long used) {}
    private record Existing(String id, String model, long units) {}
}
