package dev.peng.meterflow;

import dev.peng.meterflow.Contracts.EventView;
import dev.peng.meterflow.Contracts.IssuedKey;
import dev.peng.meterflow.Contracts.Reconciliation;
import dev.peng.meterflow.Contracts.TenantView;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

@Service
class TenantService {
    private final JdbcTemplate jdbc;

    TenantService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Transactional
    TenantView create(String name, long quotaUnits) {
        String id = UUID.randomUUID().toString();
        jdbc.update("INSERT INTO tenants(id, name, quota_units, used_units, created_at) VALUES (?, ?, ?, 0, ?)",
                id, name.trim(), quotaUnits, Timestamp.from(Instant.now()));
        return new TenantView(id, name.trim(), quotaUnits, 0, 0, quotaUnits);
    }

    TenantView get(String tenantId) {
        return jdbc.query("SELECT id, name, quota_units, used_units, reserved_units FROM tenants WHERE id = ?",
                (rs, row) -> new TenantView(rs.getString("id"), rs.getString("name"),
                        rs.getLong("quota_units"), rs.getLong("used_units"), rs.getLong("reserved_units"),
                        rs.getLong("quota_units") - rs.getLong("used_units") - rs.getLong("reserved_units")),
                tenantId)
                .stream().findFirst().orElseThrow(() -> new ApiError(HttpStatus.NOT_FOUND,
                        "TENANT_NOT_FOUND", "租户不存在"));
    }

    @Transactional
    IssuedKey issueKey(String tenantId) {
        get(tenantId);
        String id = UUID.randomUUID().toString();
        String raw = SecretHash.newKey();
        jdbc.update("INSERT INTO api_keys(id, tenant_id, secret_hash, revoked, created_at) VALUES (?, ?, ?, FALSE, ?)",
                id, tenantId, SecretHash.sha256(raw), Timestamp.from(Instant.now()));
        return new IssuedKey(id, tenantId, raw);
    }

    @Transactional
    void revokeKey(String keyId) {
        String tenantId = jdbc.query("SELECT tenant_id FROM api_keys WHERE id = ? AND revoked = FALSE",
                (rs, row) -> rs.getString("tenant_id"), keyId).stream().findFirst()
                .orElseThrow(() -> new ApiError(HttpStatus.NOT_FOUND, "KEY_NOT_FOUND", "密钥不存在或已撤销"));
        jdbc.query("SELECT id FROM tenants WHERE id = ? FOR UPDATE",
                (rs, row) -> rs.getString("id"), tenantId);
        if (jdbc.update("UPDATE api_keys SET revoked = TRUE WHERE id = ? AND revoked = FALSE", keyId) == 0) {
            throw new ApiError(HttpStatus.NOT_FOUND, "KEY_NOT_FOUND", "密钥不存在或已撤销");
        }
    }

    String tenantForKey(String raw) {
        if (raw == null || raw.length() > 256 || !raw.startsWith("mf_")) {
            throw new ApiError(HttpStatus.UNAUTHORIZED, "INVALID_API_KEY", "API 密钥无效");
        }
        return jdbc.query("SELECT tenant_id FROM api_keys WHERE secret_hash = ? AND revoked = FALSE",
                (rs, row) -> rs.getString("tenant_id"), SecretHash.sha256(raw))
                .stream().findFirst().orElseThrow(() -> new ApiError(HttpStatus.UNAUTHORIZED,
                        "INVALID_API_KEY", "API 密钥无效"));
    }

    List<EventView> events(String tenantId) {
        get(tenantId);
        return jdbc.query("SELECT id, request_id, model_name, units, created_at FROM usage_events "
                        + "WHERE tenant_id = ? ORDER BY created_at DESC, id DESC LIMIT 50",
                (rs, row) -> new EventView(rs.getString("id"), rs.getString("request_id"),
                        rs.getString("model_name"), rs.getLong("units"),
                        rs.getTimestamp("created_at").toInstant()), tenantId);
    }

    // All reads come from one MVCC snapshot. Events, reservations and the balance commit together, so the snapshot
    // is consistent without the tenant lock, and a long SUM never stalls usage writes.
    @Transactional(isolation = Isolation.REPEATABLE_READ, readOnly = true)
    Reconciliation reconcile(String tenantId) {
        long[] stored = jdbc.query("SELECT used_units, reserved_units FROM tenants WHERE id = ?",
                (rs, row) -> new long[] {rs.getLong("used_units"), rs.getLong("reserved_units")}, tenantId)
                .stream().findFirst()
                .orElseThrow(() -> new ApiError(HttpStatus.NOT_FOUND, "TENANT_NOT_FOUND", "租户不存在"));
        long ledger = jdbc.queryForObject(
                "SELECT COALESCE(SUM(units), 0) FROM usage_events WHERE tenant_id = ?", Long.class, tenantId);
        long open = jdbc.queryForObject("SELECT COALESCE(SUM(reserved_units), 0) FROM usage_reservations "
                + "WHERE tenant_id = ? AND state = 'RESERVED'", Long.class, tenantId);
        return new Reconciliation(tenantId, stored[0], ledger, stored[1], open,
                stored[0] == ledger && stored[1] == open);
    }
}
