package dev.peng.meterflow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.peng.meterflow.Contracts.UsageInput;
import dev.peng.meterflow.Contracts.UsageView;
import dev.peng.meterflow.UsageLedger.Charge;
import dev.peng.meterflow.UsageLedger.Operation;
import dev.peng.meterflow.UsageLedger.Outcome;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class UsageBatcherTest {
    /** Records batch sizes; the first batch blocks until released, like a slow commit. */
    static class SlowLedger extends UsageLedger {
        final List<Integer> batchSizes = new CopyOnWriteArrayList<>();
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        volatile RuntimeException failure;

        SlowLedger() {
            super(null, null);
        }

        @Override
        List<Outcome> apply(String tenantId, List<? extends Operation> operations) {
            batchSizes.add(operations.size());
            entered.countDown();
            try {
                release.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (failure != null) {
                throw failure;
            }
            return operations.stream().map(o -> Outcome.ok(new UsageView("e-" + o.requestId(), tenantId,
                    o.requestId(), "model-a", 1, false, 0))).toList();
        }
    }

    final SlowLedger ledger = new SlowLedger();
    final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    UsageBatcher batcher;

    @AfterEach
    void close() throws InterruptedException {
        ledger.release.countDown();
        batcher.close();
    }

    double outcomes(String outcome) {
        return meters.get("meterflow.usage.operations").tags("type", "charge", "outcome", outcome).counter().count();
    }

    static Charge charge(String requestId) {
        return new Charge("hash", new UsageInput(requestId, "model-a", 1));
    }

    @Test
    void chargesArrivingDuringACommitShareTheNextBatch() throws Exception {
        batcher = new UsageBatcher(ledger, 20, 2, 1000, meters);
        List<CompletableFuture<Object>> results = new ArrayList<>();
        results.add(batcher.submit("t1", charge("first")));
        assertThat(ledger.entered.await(5, TimeUnit.SECONDS)).isTrue();
        for (int i = 0; i < 50; i++) {
            results.add(batcher.submit("t1", charge("queued-" + i)));
        }
        ledger.release.countDown();

        for (var result : results) {
            assertThat(((UsageView) result.get(5, TimeUnit.SECONDS)).eventId()).startsWith("e-");
        }
        // One charge started alone; the 50 queued behind it went in batches capped at 20.
        assertThat(ledger.batchSizes).containsExactly(1, 20, 20, 10);
        assertThat(batcher.batches()).isEqualTo(4);
        var sizes = meters.get("meterflow.usage.batch.size").summary();
        assertThat(sizes.count()).isEqualTo(4);
        assertThat(sizes.totalAmount()).isEqualTo(51);
        assertThat(sizes.max()).isEqualTo(20);
        assertThat(outcomes("accepted")).isEqualTo(51);
        assertThat(meters.get("meterflow.usage.queue.wait").timer().count()).isEqualTo(51);
        assertThat(meters.get("meterflow.usage.batch.duration").timer().count()).isEqualTo(4);
        assertThat(meters.get("meterflow.usage.pending").gauge().value()).isZero();
    }

    @Test
    void fullTenantBacklogIsRejectedWithoutBlocking() throws Exception {
        batcher = new UsageBatcher(ledger, 10, 1, 2, meters);
        var first = batcher.submit("t1", charge("in-flight"));
        assertThat(ledger.entered.await(5, TimeUnit.SECONDS)).isTrue();
        var second = batcher.submit("t1", charge("queued-1"));
        var third = batcher.submit("t1", charge("queued-2"));

        assertThatThrownBy(() -> batcher.submit("t1", charge("overflow")))
                .isInstanceOf(ApiError.class)
                .satisfies(e -> assertThat(((ApiError) e).code()).isEqualTo("USAGE_BACKLOG_FULL"));
        assertThat(outcomes("backlog_full")).isEqualTo(1);
        assertThat(meters.get("meterflow.usage.pending").gauge().value()).isEqualTo(2);
        // Other tenants have their own backlog.
        var otherTenant = batcher.submit("t2", charge("other"));
        ledger.release.countDown();
        for (var result : List.of(first, second, third, otherTenant)) {
            assertThat(result.get(5, TimeUnit.SECONDS)).isNotNull();
        }
    }

    @Test
    void failedTransactionFailsEveryChargeInTheBatch() throws Exception {
        batcher = new UsageBatcher(ledger, 10, 1, 100, meters);
        ledger.failure = new IllegalStateException("commit failed");
        var first = batcher.submit("t1", charge("a"));
        assertThat(ledger.entered.await(5, TimeUnit.SECONDS)).isTrue();
        var second = batcher.submit("t1", charge("b"));
        ledger.release.countDown();

        for (var result : List.of(first, second)) {
            assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasRootCauseMessage("commit failed");
        }
        assertThat(outcomes("failed")).isEqualTo(2);
    }

    @Test
    void submissionsAfterCloseAreRefused() throws Exception {
        batcher = new UsageBatcher(ledger, 10, 1, 100, meters);
        ledger.release.countDown();
        batcher.close();

        assertThatThrownBy(() -> batcher.submit("t1", charge("late")))
                .isInstanceOf(ApiError.class)
                .satisfies(e -> assertThat(((ApiError) e).code()).isEqualTo("SHUTTING_DOWN"));
    }
}
