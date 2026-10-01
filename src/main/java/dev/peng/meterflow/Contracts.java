package dev.peng.meterflow;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.time.Instant;

public final class Contracts {
    private Contracts() {}

    public record CreateTenant(@NotBlank @Size(max = 100) String name,
                        @NotNull @Min(0) @Max(1_000_000_000_000L) Long quotaUnits) {}

    public record TenantView(String id, String name, long quotaUnits, long usedUnits, long reservedUnits,
                             long remainingUnits) {}

    public record IssuedKey(String id, String tenantId, String apiKey) {}

    public record UsageInput(@NotBlank @Size(max = 100) String requestId,
                      @NotBlank @Size(max = 100) String model,
                      @Min(1) @Max(1_000_000_000L) long units) {}

    public record UsageView(String eventId, String tenantId, String requestId,
                     String model, long units, boolean replayed, long remainingUnits) {}

    /** {@code ttlSeconds} defaults to {@code meterflow.usage.reservation.default-ttl}. */
    public record ReserveInput(@NotBlank @Size(max = 100) String requestId,
                               @NotBlank @Size(max = 100) String model,
                               @Min(1) @Max(1_000_000_000L) long units,
                               @Min(1) @Max(3600) Integer ttlSeconds) {}

    public record CommitInput(@Min(1) @Max(1_000_000_000L) long units) {}

    /** {@code state} is RESERVED, COMMITTED, RELEASED or EXPIRED; committed fields are set once committed. */
    public record ReservationView(String requestId, String tenantId, String model, long reservedUnits,
                                  String state, Long committedUnits, String eventId, Instant expiresAt,
                                  boolean replayed, long remainingUnits) {}

    public record EventView(String id, String requestId, String model, long units, Instant createdAt) {}

    /** Checks used units against the event ledger and reserved units against open reservations. */
    public record Reconciliation(String tenantId, long storedUnits, long ledgerUnits,
                                 long storedReservedUnits, long openReservedUnits, boolean consistent) {}
}
