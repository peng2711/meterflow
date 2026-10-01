package dev.peng.meterflow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.httpBasic;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import dev.peng.meterflow.Contracts.TenantView;
import dev.peng.meterflow.Contracts.UsageInput;
import dev.peng.meterflow.UsageLedger.Charge;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest
@AutoConfigureMockMvc
@TestPropertySource(properties = {
        "spring.datasource.url=jdbc:h2:mem:meterflowtest;MODE=MySQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1",
        "debug=false",
        "logging.level.root=ERROR",
        "meterflow.admin.username=test-admin",
        "meterflow.admin.password=test-password"
})
class MeterflowIntegrationTest {
    @Autowired TenantService tenants;
    @Autowired UsageService usage;
    @Autowired UsageLedger ledger;
    @Autowired JdbcTemplate jdbc;
    @Autowired MockMvc mvc;

    @BeforeEach
    void clean() {
        jdbc.update("DELETE FROM usage_events");
        jdbc.update("DELETE FROM api_keys");
        jdbc.update("DELETE FROM tenants");
    }

    @Test
    void retryDoesNotChargeTwiceAndChangedPayloadConflicts() {
        TenantView tenant = tenants.create("alpha", 10);
        String key = tenants.issueKey(tenant.id()).apiKey();
        assertThat(jdbc.queryForObject("SELECT secret_hash FROM api_keys WHERE tenant_id = ?",
                String.class, tenant.id())).isEqualTo(SecretHash.sha256(key)).isNotEqualTo(key);
        UsageInput input = new UsageInput("call-1", "example-model", 7);

        var first = usage.record(key, input);
        var retry = usage.record(key, input);

        assertThat(retry.eventId()).isEqualTo(first.eventId());
        assertThat(retry.replayed()).isTrue();
        assertThat(tenants.get(tenant.id()).usedUnits()).isEqualTo(7);
        assertThat(tenants.reconcile(tenant.id()).consistent()).isTrue();
        assertThatThrownBy(() -> usage.record(key, new UsageInput("call-1", "example-model", 8)))
                .isInstanceOf(ApiError.class)
                .satisfies(e -> assertThat(((ApiError) e).code()).isEqualTo("IDEMPOTENCY_CONFLICT"));
        assertThat(tenants.get(tenant.id()).usedUnits()).isEqualTo(7);
    }

    @Test
    void quotaAndKeysAreTenantScoped() {
        TenantView alpha = tenants.create("alpha", 3);
        TenantView beta = tenants.create("beta", 3);
        var alphaKey = tenants.issueKey(alpha.id());
        String betaKey = tenants.issueKey(beta.id()).apiKey();

        usage.record(alphaKey.apiKey(), new UsageInput("same-id", "model-a", 3));
        usage.record(betaKey, new UsageInput("same-id", "model-a", 2));

        assertThat(tenants.get(alpha.id()).usedUnits()).isEqualTo(3);
        assertThat(tenants.get(beta.id()).usedUnits()).isEqualTo(2);
        assertThatThrownBy(() -> usage.record(alphaKey.apiKey(), new UsageInput("new-id", "model-a", 1)))
                .isInstanceOf(ApiError.class)
                .satisfies(e -> assertThat(((ApiError) e).code()).isEqualTo("QUOTA_EXCEEDED"));
        tenants.revokeKey(alphaKey.id());
        assertThatThrownBy(() -> usage.record(alphaKey.apiKey(), new UsageInput("new-id", "model-a", 1)))
                .isInstanceOf(ApiError.class)
                .satisfies(e -> assertThat(((ApiError) e).code()).isEqualTo("INVALID_API_KEY"));
    }

    @Test
    void concurrentChargesCannotExceedQuota() throws Exception {
        TenantView tenant = tenants.create("parallel", 1);
        String key = tenants.issueKey(tenant.id()).apiKey();
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        var pool = Executors.newFixedThreadPool(2);
        try {
            List<Callable<String>> calls = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                String requestId = "parallel-" + i;
                calls.add(() -> {
                    ready.countDown();
                    if (!start.await(5, TimeUnit.SECONDS)) throw new AssertionError("start timeout");
                    try {
                        usage.record(key, new UsageInput(requestId, "model-a", 1));
                        return "accepted";
                    } catch (ApiError error) {
                        return error.code();
                    }
                });
            }
            var first = pool.submit(calls.get(0));
            var second = pool.submit(calls.get(1));
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            assertThat(List.of(first.get(10, TimeUnit.SECONDS), second.get(10, TimeUnit.SECONDS)))
                    .containsExactlyInAnyOrder("accepted", "QUOTA_EXCEEDED");
        } finally {
            pool.shutdownNow();
        }
        assertThat(tenants.get(tenant.id()).usedUnits()).isEqualTo(1);
        assertThat(tenants.reconcile(tenant.id()).ledgerUnits()).isEqualTo(1);
    }

    @Test
    void simultaneousRetriesCreateOneEvent() throws Exception {
        TenantView tenant = tenants.create("retry", 5);
        String key = tenants.issueKey(tenant.id()).apiKey();
        CountDownLatch start = new CountDownLatch(1);
        var pool = Executors.newFixedThreadPool(2);
        try {
            Callable<Boolean> call = () -> {
                if (!start.await(5, TimeUnit.SECONDS)) throw new AssertionError("start timeout");
                return usage.record(key, new UsageInput("same-call", "model-a", 3)).replayed();
            };
            var first = pool.submit(call);
            var second = pool.submit(call);
            start.countDown();
            assertThat(List.of(first.get(10, TimeUnit.SECONDS), second.get(10, TimeUnit.SECONDS)))
                    .containsExactlyInAnyOrder(false, true);
        } finally {
            pool.shutdownNow();
        }
        assertThat(tenants.get(tenant.id()).usedUnits()).isEqualTo(3);
        assertThat(tenants.events(tenant.id())).hasSize(1);
    }

    @Test
    void chargesInOneBatchAreDecidedInOrder() {
        TenantView tenant = tenants.create("batch", 10);
        String key = tenants.issueKey(tenant.id()).apiKey();
        var revoked = tenants.issueKey(tenant.id());
        tenants.revokeKey(revoked.id());
        String hash = SecretHash.sha256(key);

        var outcomes = ledger.apply(tenant.id(), List.of(
                new Charge(hash, new UsageInput("r1", "model-a", 4)),
                new Charge(hash, new UsageInput("r1", "model-a", 4)),
                new Charge(hash, new UsageInput("r1", "model-a", 5)),
                new Charge(SecretHash.sha256(revoked.apiKey()), new UsageInput("r2", "model-a", 1)),
                new Charge(hash, new UsageInput("r3", "model-a", 7)),
                new Charge(hash, new UsageInput("r4", "model-a", 6))));

        assertThat(outcomes.get(0).view().replayed()).isFalse();
        assertThat(outcomes.get(0).view().remainingUnits()).isEqualTo(6);
        assertThat(outcomes.get(1).view().replayed()).isTrue();
        assertThat(outcomes.get(1).view().eventId()).isEqualTo(outcomes.get(0).view().eventId());
        assertThat(outcomes.subList(2, 5)).extracting(o -> o.error().code())
                .containsExactly("IDEMPOTENCY_CONFLICT", "INVALID_API_KEY", "QUOTA_EXCEEDED");
        assertThat(outcomes.get(5).view().remainingUnits()).isZero();
        assertThat(tenants.get(tenant.id()).usedUnits()).isEqualTo(10);
        assertThat(tenants.events(tenant.id())).hasSize(2);
        assertThat(tenants.reconcile(tenant.id()).consistent()).isTrue();
    }

    @Test
    void burstOnOneTenantChargesEachRequestOnceUpToQuota() throws Exception {
        TenantView tenant = tenants.create("burst", 150);
        String key = tenants.issueKey(tenant.id()).apiKey();

        var outcomes = Burst.twice(usage, key, 200, 32);

        // 150 ids fit: each is accepted once and replayed once. The other 50 are rejected both times.
        assertThat(outcomes).containsOnly(entry("accepted", 150L), entry("replayed", 150L),
                entry("QUOTA_EXCEEDED", 100L));
        assertThat(tenants.get(tenant.id()).usedUnits()).isEqualTo(150);
        assertThat(tenants.reconcile(tenant.id()).consistent()).isTrue();
    }

    @Test
    void httpAuthValidationAndUsageContract() throws Exception {
        mvc.perform(post("/admin/tenants").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"alpha\",\"quotaUnits\":2}"))
                .andExpect(status().isUnauthorized());

        mvc.perform(post("/admin/tenants").with(httpBasic("test-admin", "test-password"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"\",\"quotaUnits\":-1}"))
                .andExpect(status().isBadRequest()).andExpect(jsonPath("$.code").value("INVALID_INPUT"));

        TenantView tenant = tenants.create("alpha", 2);
        String key = tenants.issueKey(tenant.id()).apiKey();
        mvc.perform(post("/v1/usage").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"requestId\":\"r1\",\"model\":\"m1\",\"units\":1}"))
                .andExpect(status().isUnauthorized());
        mvc.perform(post("/v1/usage").header("X-Api-Key", key)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"requestId\":\"r1\",\"model\":\"m1\",\"units\":1}"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.remainingUnits").value(1));
    }
}
