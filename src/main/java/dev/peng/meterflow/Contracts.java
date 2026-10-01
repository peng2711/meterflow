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

    public record TenantView(String id, String name, long quotaUnits, long usedUnits, long remainingUnits) {}

    public record IssuedKey(String id, String tenantId, String apiKey) {}

    public record UsageInput(@NotBlank @Size(max = 100) String requestId,
                      @NotBlank @Size(max = 100) String model,
                      @Min(1) @Max(1_000_000_000L) long units) {}

    public record UsageView(String eventId, String tenantId, String requestId,
                     String model, long units, boolean replayed, long remainingUnits) {}

    public record EventView(String id, String requestId, String model, long units, Instant createdAt) {}

    public record Reconciliation(String tenantId, long storedUnits, long ledgerUnits, boolean consistent) {}
}
