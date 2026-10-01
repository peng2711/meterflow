package dev.peng.meterflow;

import dev.peng.meterflow.Contracts.UsageInput;
import dev.peng.meterflow.Contracts.UsageView;
import dev.peng.meterflow.UsageLedger.Charge;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

@Service
class UsageService {
    private final TenantService tenants;
    private final UsageBatcher batcher;
    private final Duration responseTimeout;
    private final Counter timeouts;

    UsageService(TenantService tenants, UsageBatcher batcher,
                 @Value("${meterflow.usage.response-timeout}") Duration responseTimeout, MeterRegistry meters) {
        this.tenants = tenants;
        this.batcher = batcher;
        this.responseTimeout = responseTimeout;
        // Counted apart from charge outcomes: a timed-out charge may still commit later.
        this.timeouts = Counter.builder("meterflow.usage.response.timeouts")
                .description("Requests that stopped waiting before their batch reported a result")
                .register(meters);
    }

    UsageView record(String apiKey, UsageInput input) {
        String tenantId = tenants.tenantForKey(apiKey);
        var result = batcher.submit(tenantId, new Charge(SecretHash.sha256(apiKey), input));
        try {
            return result.get(responseTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException failure) {
                throw failure;
            }
            throw new IllegalStateException("用量写入失败", e.getCause());
        } catch (TimeoutException e) {
            timeouts.increment();
            // The charge may still commit later; retrying with the same requestId is safe either way.
            throw new ApiError(HttpStatus.SERVICE_UNAVAILABLE, "USAGE_PENDING", "上报结果未确认，请用相同 requestId 重试");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ApiError(HttpStatus.SERVICE_UNAVAILABLE, "USAGE_PENDING", "上报结果未确认，请用相同 requestId 重试");
        }
    }
}
