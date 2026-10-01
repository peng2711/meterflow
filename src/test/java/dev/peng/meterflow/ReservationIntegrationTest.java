package dev.peng.meterflow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import dev.peng.meterflow.Contracts.CommitInput;
import dev.peng.meterflow.Contracts.ReservationView;
import dev.peng.meterflow.Contracts.ReserveInput;
import dev.peng.meterflow.Contracts.TenantView;
import dev.peng.meterflow.Contracts.UsageInput;
import dev.peng.meterflow.UsageLedger.Commit;
import dev.peng.meterflow.UsageLedger.Release;
import dev.peng.meterflow.UsageLedger.Reserve;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest
@AutoConfigureMockMvc
@TestPropertySource(properties = {
        "spring.datasource.url=jdbc:h2:mem:reservationtest;MODE=MySQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1",
        "logging.level.root=ERROR",
        "meterflow.admin.password=test-password",
        // Tests call the sweeper themselves.
        "meterflow.usage.reservation.sweep-interval=1h"
})
class ReservationIntegrationTest {
    @TestConfiguration
    static class TestClock {
        @Bean
        @Primary
        MutableClock testClock() {
            return new MutableClock();
        }
    }

    @Autowired TenantService tenants;
    @Autowired UsageService usage;
    @Autowired UsageLedger ledger;
    @Autowired ReservationSweeper sweeper;
    @Autowired MutableClock clock;
    @Autowired JdbcTemplate jdbc;
    @Autowired MockMvc mvc;

    TenantView tenant;
    String key;

    @BeforeEach
    void setUp() {
        jdbc.update("DELETE FROM usage_reservations");
        jdbc.update("DELETE FROM usage_events");
        jdbc.update("DELETE FROM api_keys");
        jdbc.update("DELETE FROM tenants");
        tenant = tenants.create("reserving", 100);
        key = tenants.issueKey(tenant.id()).apiKey();
    }

    ReservationView reserve(String requestId, long units) {
        return usage.reserve(key, new ReserveInput(requestId, "model-a", units, null));
    }

    static void assertCode(ThrowingCallable call, String code) {
        assertThatThrownBy(call).isInstanceOf(ApiError.class)
                .satisfies(e -> assertThat(((ApiError) e).code()).isEqualTo(code));
    }

    void assertBalance(long used, long reserved) {
        TenantView account = tenants.get(tenant.id());
        assertThat(account.usedUnits()).isEqualTo(used);
        assertThat(account.reservedUnits()).isEqualTo(reserved);
        assertThat(account.remainingUnits()).isEqualTo(100 - used - reserved);
        assertThat(tenants.reconcile(tenant.id()).consistent()).isTrue();
    }

    @Test
    void reservedQuotaIsHeldUntilCommitChargesTheActualUsage() {
        var held = reserve("call-1", 60);
        assertThat(held.state()).isEqualTo("RESERVED");
        assertThat(held.remainingUnits()).isEqualTo(40);
        assertBalance(0, 60);
        // Held quota is not available to anyone else, reservation or direct charge.
        assertCode(() -> reserve("call-2", 50), "QUOTA_EXCEEDED");
        assertCode(() -> usage.record(key, new UsageInput("direct", "model-a", 41)), "QUOTA_EXCEEDED");

        var committed = usage.commit(key, "call-1", new CommitInput(45));

        assertThat(committed.state()).isEqualTo("COMMITTED");
        assertThat(committed.committedUnits()).isEqualTo(45);
        assertThat(committed.remainingUnits()).isEqualTo(55);
        assertBalance(45, 0);
        assertThat(tenants.events(tenant.id())).singleElement()
                .satisfies(e -> {
                    assertThat(e.requestId()).isEqualTo("call-1");
                    assertThat(e.units()).isEqualTo(45);
                    assertThat(e.id()).isEqualTo(committed.eventId());
                });
    }

    @Test
    void retriesAreIdempotentAndClosedReservationsRejectTheOtherSettlement() {
        reserve("c", 20);
        usage.commit(key, "c", new CommitInput(15));
        assertThat(usage.commit(key, "c", new CommitInput(15)).replayed()).isTrue();
        assertThat(reserve("c", 20).replayed()).isTrue();
        assertCode(() -> usage.commit(key, "c", new CommitInput(14)), "IDEMPOTENCY_CONFLICT");
        assertCode(() -> reserve("c", 21), "IDEMPOTENCY_CONFLICT");
        assertCode(() -> usage.release(key, "c"), "RESERVATION_COMMITTED");

        reserve("r", 30);
        assertThat(usage.release(key, "r").state()).isEqualTo("RELEASED");
        assertThat(usage.release(key, "r").replayed()).isTrue();
        assertCode(() -> usage.commit(key, "r", new CommitInput(1)), "RESERVATION_RELEASED");
        assertCode(() -> usage.commit(key, "missing", new CommitInput(1)), "RESERVATION_NOT_FOUND");
        assertBalance(15, 0);
    }

    @Test
    void commitAboveTheReservationIsRejectedAndLeavesItOpen() {
        reserve("big", 10);

        assertCode(() -> usage.commit(key, "big", new CommitInput(11)), "COMMIT_EXCEEDS_RESERVATION");
        assertBalance(0, 10);

        assertThat(usage.commit(key, "big", new CommitInput(10)).state()).isEqualTo("COMMITTED");
        assertBalance(10, 0);
    }

    @Test
    void expiryIsDecidedByTimeAndTheSweepReturnsAbandonedQuota() {
        usage.reserve(key, new ReserveInput("late", "model-a", 30, 60));
        usage.reserve(key, new ReserveInput("abandoned", "model-a", 20, 60));
        usage.reserve(key, new ReserveInput("fresh", "model-a", 10, 600));
        clock.advance(Duration.ofSeconds(61));

        // Expired before the sweep ran: the late commit is still refused, and its quota comes back.
        assertCode(() -> usage.commit(key, "late", new CommitInput(5)), "RESERVATION_EXPIRED");
        assertBalance(0, 30);

        assertThat(sweeper.sweep()).isEqualTo(1);
        assertBalance(0, 10);
        assertThat(reserve("abandoned", 20).state()).isEqualTo("EXPIRED");
        assertThat(usage.release(key, "abandoned").replayed()).isTrue();
        assertThat(sweeper.sweep()).isZero();
        assertThat(usage.commit(key, "fresh", new CommitInput(10)).state()).isEqualTo("COMMITTED");
        assertBalance(10, 0);
    }

    @Test
    void chargesAndReservationsShareRequestIds() {
        usage.record(key, new UsageInput("charged", "model-a", 1));
        reserve("reserved", 1);

        assertCode(() -> reserve("charged", 1), "IDEMPOTENCY_CONFLICT");
        assertCode(() -> usage.record(key, new UsageInput("reserved", "model-a", 1)), "IDEMPOTENCY_CONFLICT");
        assertBalance(1, 1);
    }

    @Test
    void operationsOnTheSameRequestInOneBatchSeeEachOther() {
        String hash = SecretHash.sha256(key);
        var outcomes = ledger.apply(tenant.id(), List.of(
                new Reserve(hash, "r1", "model-a", 40, Duration.ofMinutes(5)),
                new Commit(hash, "r1", 25),
                new Release(hash, "r1"),
                new Reserve(hash, "r2", "model-a", 80, Duration.ofMinutes(5)),
                new Reserve(hash, "r3", "model-a", 75, Duration.ofMinutes(5)),
                new Release(hash, "r3")));

        assertThat(outcomes.get(0).view()).isInstanceOf(ReservationView.class);
        assertThat(((ReservationView) outcomes.get(1).view()).state()).isEqualTo("COMMITTED");
        assertThat(outcomes.get(2).error().code()).isEqualTo("RESERVATION_COMMITTED");
        // 25 used leaves 75: r2 does not fit, r3 takes all of it and is released again.
        assertThat(outcomes.get(3).error().code()).isEqualTo("QUOTA_EXCEEDED");
        assertThat(((ReservationView) outcomes.get(5).view()).state()).isEqualTo("RELEASED");
        assertBalance(25, 0);
    }

    @Test
    void concurrentReservationsNeverHoldMoreThanTheQuota() throws Exception {
        CountDownLatch start = new CountDownLatch(1);
        var pool = Executors.newFixedThreadPool(32);
        List<Future<String>> results = new ArrayList<>();
        try {
            for (int i = 0; i < 40; i++) {
                String requestId = "parallel-" + i;
                results.add(pool.submit(() -> {
                    start.await(5, TimeUnit.SECONDS);
                    try {
                        reserve(requestId, 10);
                        usage.commit(key, requestId, new CommitInput(4));
                        return "committed";
                    } catch (ApiError error) {
                        return error.code();
                    }
                }));
            }
            start.countDown();
            List<String> outcomes = new ArrayList<>();
            for (var result : results) {
                outcomes.add(result.get(30, TimeUnit.SECONDS));
            }
            // Each holds 10 while in flight but keeps only 4, so between 10 and 25 calls get through.
            long committed = outcomes.stream().filter("committed"::equals).count();
            assertThat(committed).isBetween(10L, 25L);
            assertThat(outcomes).containsOnly("committed", "QUOTA_EXCEEDED");
            assertBalance(committed * 4, 0);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void httpEndpointsAuthenticateAndValidate() throws Exception {
        mvc.perform(post("/v1/reservations").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"requestId\":\"h1\",\"model\":\"m\",\"units\":5}"))
                .andExpect(status().isUnauthorized());
        mvc.perform(post("/v1/reservations").header("X-Api-Key", key).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"requestId\":\"h1\",\"model\":\"m\",\"units\":5,\"ttlSeconds\":7200}"))
                .andExpect(status().isBadRequest()).andExpect(jsonPath("$.code").value("INVALID_INPUT"));
        mvc.perform(post("/v1/reservations").header("X-Api-Key", key).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"requestId\":\"h1\",\"model\":\"m\",\"units\":5}"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.state").value("RESERVED"))
                .andExpect(jsonPath("$.remainingUnits").value(95));
        mvc.perform(post("/v1/reservations/h1/commit").header("X-Api-Key", key)
                        .contentType(MediaType.APPLICATION_JSON).content("{\"units\":6}"))
                .andExpect(status().isUnprocessableEntity());
        mvc.perform(post("/v1/reservations/h1/commit").header("X-Api-Key", key)
                        .contentType(MediaType.APPLICATION_JSON).content("{\"units\":3}"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.committedUnits").value(3));
        mvc.perform(post("/v1/reservations/h1/release").header("X-Api-Key", key))
                .andExpect(status().isConflict()).andExpect(jsonPath("$.code").value("RESERVATION_COMMITTED"));
    }
}
