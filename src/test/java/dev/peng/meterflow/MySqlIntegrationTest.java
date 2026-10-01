package dev.peng.meterflow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

import dev.peng.meterflow.Contracts.UsageInput;
import java.sql.Connection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

@SpringBootTest(properties = {"debug=false", "logging.level.root=ERROR",
        "meterflow.admin.password=test-password"})
@EnabledIfEnvironmentVariable(named = "METERFLOW_MYSQL_TEST_URL", matches = ".+")
class MySqlIntegrationTest {
    @DynamicPropertySource
    static void database(DynamicPropertyRegistry properties) {
        properties.add("spring.datasource.url", () -> System.getenv("METERFLOW_MYSQL_TEST_URL"));
        properties.add("spring.datasource.username", () -> System.getenv("METERFLOW_MYSQL_TEST_USER"));
        properties.add("spring.datasource.password", () -> System.getenv("METERFLOW_MYSQL_TEST_PASSWORD"));
    }

    @Autowired TenantService tenants;
    @Autowired UsageService usage;
    @Autowired JdbcTemplate jdbc;
    @Autowired DataSource dataSource;

    @BeforeEach
    void clean() {
        removeFaultConstraint();
        jdbc.update("DELETE FROM usage_events");
        jdbc.update("DELETE FROM api_keys");
        jdbc.update("DELETE FROM tenants");
    }

    @AfterEach
    void removeFaultConstraint() {
        Integer count = jdbc.queryForObject("SELECT COUNT(*) FROM information_schema.table_constraints "
                        + "WHERE table_schema = DATABASE() AND table_name = 'tenants' "
                        + "AND constraint_name = 'ck_fault_zero'", Integer.class);
        if (count != null && count > 0) {
            jdbc.execute("ALTER TABLE tenants DROP CHECK ck_fault_zero");
        }
    }

    @Test
    void migrationAndIdempotentChargeWorkOnMySql() {
        var tenant = tenants.create("mysql", 5);
        String key = tenants.issueKey(tenant.id()).apiKey();
        var input = new UsageInput("mysql-1", "model-a", 4);

        var first = usage.record(key, input);
        var replay = usage.record(key, input);

        assertThat(replay.eventId()).isEqualTo(first.eventId());
        assertThat(replay.replayed()).isTrue();
        assertThat(tenants.reconcile(tenant.id()).ledgerUnits()).isEqualTo(4);
        assertThat(tenants.reconcile(tenant.id()).consistent()).isTrue();
        assertThatThrownBy(() -> usage.record(key, new UsageInput("mysql-2", "model-a", 2)))
                .isInstanceOf(ApiError.class)
                .satisfies(e -> assertThat(((ApiError) e).code()).isEqualTo("QUOTA_EXCEEDED"));
    }

    @Test
    void concurrentRetriesAndBudgetLimitRemainConsistent() throws Exception {
        var tenant = tenants.create("parallel", 1);
        String key = tenants.issueKey(tenant.id()).apiKey();
        CountDownLatch start = new CountDownLatch(1);
        var pool = Executors.newFixedThreadPool(2);
        try {
            Callable<Boolean> retry = () -> {
                if (!start.await(5, TimeUnit.SECONDS)) throw new AssertionError("start timeout");
                return usage.record(key, new UsageInput("same", "model-a", 1)).replayed();
            };
            var first = pool.submit(retry);
            var second = pool.submit(retry);
            start.countDown();
            assertThat(List.of(first.get(10, TimeUnit.SECONDS), second.get(10, TimeUnit.SECONDS)))
                    .containsExactlyInAnyOrder(false, true);
        } finally {
            pool.shutdownNow();
        }
        assertThat(tenants.get(tenant.id()).usedUnits()).isEqualTo(1);
        assertThat(tenants.events(tenant.id())).hasSize(1);
        assertThatThrownBy(() -> usage.record(key, new UsageInput("different", "model-a", 1)))
                .isInstanceOf(ApiError.class)
                .satisfies(e -> assertThat(((ApiError) e).code()).isEqualTo("QUOTA_EXCEEDED"));
    }

    @Test
    void burstOnOneTenantStaysExactOnMySql() throws Exception {
        var tenant = tenants.create("burst", 150);
        String key = tenants.issueKey(tenant.id()).apiKey();

        var outcomes = Burst.twice(usage, key, 200, 64);

        assertThat(outcomes).containsOnly(entry("accepted", 150L), entry("replayed", 150L),
                entry("QUOTA_EXCEEDED", 100L));
        assertThat(tenants.get(tenant.id()).usedUnits()).isEqualTo(150);
        assertThat(tenants.reconcile(tenant.id()).consistent()).isTrue();
    }

    @Test
    void reconciliationDoesNotWaitForTheTenantLock() throws Exception {
        var tenant = tenants.create("locked", 5);
        String key = tenants.issueKey(tenant.id()).apiKey();
        usage.record(key, new UsageInput("before-lock", "model-a", 2));

        // Hold the tenant row the way an in-flight batch does.
        try (Connection holder = dataSource.getConnection()) {
            holder.setAutoCommit(false);
            try (var lock = holder.prepareStatement("SELECT id FROM tenants WHERE id = ? FOR UPDATE")) {
                lock.setString(1, tenant.id());
                lock.executeQuery().close();
            }
            var pool = Executors.newSingleThreadExecutor();
            try {
                var result = pool.submit(() -> tenants.reconcile(tenant.id())).get(5, TimeUnit.SECONDS);
                assertThat(result.storedUnits()).isEqualTo(2);
                assertThat(result.consistent()).isTrue();
            } finally {
                pool.shutdownNow();
                holder.rollback();
            }
        }
    }

    @Test
    void reconciliationStaysConsistentDuringConcurrentWrites() throws Exception {
        var tenant = tenants.create("busy", 1_000);
        String key = tenants.issueKey(tenant.id()).apiKey();
        var writer = Executors.newSingleThreadExecutor();
        try {
            var burst = writer.submit(() -> Burst.twice(usage, key, 400, 32));
            int checks = 0;
            while (!burst.isDone()) {
                assertThat(tenants.reconcile(tenant.id()).consistent()).isTrue();
                checks++;
            }
            burst.get(30, TimeUnit.SECONDS);
            assertThat(checks).isPositive();
        } finally {
            writer.shutdownNow();
        }
        assertThat(tenants.get(tenant.id()).usedUnits()).isEqualTo(400);
        assertThat(tenants.reconcile(tenant.id()).consistent()).isTrue();
    }

    @Test
    void eventInsertRollsBackWhenBalanceUpdateFails() {
        var tenant = tenants.create("rollback", 10);
        String key = tenants.issueKey(tenant.id()).apiKey();
        jdbc.execute("ALTER TABLE tenants ADD CONSTRAINT ck_fault_zero CHECK (used_units = 0)");

        assertThatThrownBy(() -> usage.record(key, new UsageInput("will-fail", "model-a", 2)))
                .isInstanceOf(org.springframework.dao.DataAccessException.class);
        assertThat(tenants.get(tenant.id()).usedUnits()).isZero();
        assertThat(tenants.events(tenant.id())).isEmpty();
        assertThat(tenants.reconcile(tenant.id()).consistent()).isTrue();
    }
}
