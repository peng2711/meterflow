package dev.peng.meterflow;

import dev.peng.meterflow.Contracts.CommitInput;
import dev.peng.meterflow.Contracts.ReservationView;
import dev.peng.meterflow.Contracts.ReserveInput;
import dev.peng.meterflow.Contracts.UsageInput;
import dev.peng.meterflow.Contracts.UsageView;
import dev.peng.meterflow.UsageLedger.Charge;
import dev.peng.meterflow.UsageLedger.Commit;
import dev.peng.meterflow.UsageLedger.Operation;
import dev.peng.meterflow.UsageLedger.Release;
import dev.peng.meterflow.UsageLedger.Reserve;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

@Service
class UsageService {
    private final TenantService tenants;
    private final UsageBatcher batcher;
    private final Duration responseTimeout;
    private final Duration defaultReservationTtl;
    private final Counter timeouts;

    UsageService(TenantService tenants, UsageBatcher batcher,
                 @Value("${meterflow.usage.response-timeout}") Duration responseTimeout,
                 @Value("${meterflow.usage.reservation.default-ttl}") Duration defaultReservationTtl,
                 MeterRegistry meters) {
        if (defaultReservationTtl.isNegative() || defaultReservationTtl.isZero()
                || defaultReservationTtl.compareTo(Duration.ofHours(1)) > 0) {
            throw new IllegalArgumentException("reservation default-ttl must be within (0, 1h]");
        }
        this.tenants = tenants;
        this.batcher = batcher;
        this.responseTimeout = responseTimeout;
        this.defaultReservationTtl = defaultReservationTtl;
        // Counted apart from operation outcomes: a timed-out operation may still commit later.
        this.timeouts = Counter.builder("meterflow.usage.response.timeouts")
                .description("Requests that stopped waiting before their batch reported a result")
                .register(meters);
    }

    UsageView record(String apiKey, UsageInput input) {
        return (UsageView) run(apiKey, hash -> new Charge(hash, input));
    }

    ReservationView reserve(String apiKey, ReserveInput input) {
        Duration ttl = input.ttlSeconds() == null ? defaultReservationTtl : Duration.ofSeconds(input.ttlSeconds());
        return (ReservationView) run(apiKey,
                hash -> new Reserve(hash, input.requestId(), input.model(), input.units(), ttl));
    }

    ReservationView commit(String apiKey, String requestId, CommitInput input) {
        checkRequestId(requestId);
        return (ReservationView) run(apiKey, hash -> new Commit(hash, requestId, input.units()));
    }

    ReservationView release(String apiKey, String requestId) {
        checkRequestId(requestId);
        return (ReservationView) run(apiKey, hash -> new Release(hash, requestId));
    }

    private Object run(String apiKey, Function<String, Operation> operation) {
        String tenantId = tenants.tenantForKey(apiKey);
        var result = batcher.submit(tenantId, operation.apply(SecretHash.sha256(apiKey)));
        try {
            return result.get(responseTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException failure) {
                throw failure;
            }
            throw new IllegalStateException("用量写入失败", e.getCause());
        } catch (TimeoutException e) {
            timeouts.increment();
            // The operation may still commit later; retrying with the same requestId is safe either way.
            throw pending();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw pending();
        }
    }

    private static void checkRequestId(String requestId) {
        if (requestId == null || requestId.isBlank() || requestId.length() > 100) {
            throw new ApiError(HttpStatus.BAD_REQUEST, "INVALID_INPUT", "请求字段无效");
        }
    }

    private static ApiError pending() {
        return new ApiError(HttpStatus.SERVICE_UNAVAILABLE, "USAGE_PENDING", "结果未确认，请用相同 requestId 重试");
    }
}
