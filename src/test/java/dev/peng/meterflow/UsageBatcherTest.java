package dev.peng.meterflow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.peng.meterflow.Contracts.UsageInput;
import dev.peng.meterflow.Contracts.UsageView;
import dev.peng.meterflow.UsageLedger.Charge;
import dev.peng.meterflow.UsageLedger.Outcome;
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
            super(null);
        }

        @Override
        List<Outcome> apply(String tenantId, List<Charge> charges) {
            batchSizes.add(charges.size());
            entered.countDown();
            try {
                release.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (failure != null) {
                throw failure;
            }
            return charges.stream().map(c -> Outcome.ok(new UsageView("e-" + c.input().requestId(), tenantId,
                    c.input().requestId(), c.input().model(), c.input().units(), false, 0))).toList();
        }
    }

    final SlowLedger ledger = new SlowLedger();
    UsageBatcher batcher;

    @AfterEach
    void close() throws InterruptedException {
        ledger.release.countDown();
        batcher.close();
    }

    static Charge charge(String requestId) {
        return new Charge("hash", new UsageInput(requestId, "model-a", 1));
    }

    @Test
    void chargesArrivingDuringACommitShareTheNextBatch() throws Exception {
        batcher = new UsageBatcher(ledger, 20, 2, 1000);
        List<CompletableFuture<UsageView>> results = new ArrayList<>();
        results.add(batcher.submit("t1", charge("first")));
        assertThat(ledger.entered.await(5, TimeUnit.SECONDS)).isTrue();
        for (int i = 0; i < 50; i++) {
            results.add(batcher.submit("t1", charge("queued-" + i)));
        }
        ledger.release.countDown();

        for (var result : results) {
            assertThat(result.get(5, TimeUnit.SECONDS).eventId()).startsWith("e-");
        }
        // One charge started alone; the 50 queued behind it went in batches capped at 20.
        assertThat(ledger.batchSizes).containsExactly(1, 20, 20, 10);
        assertThat(batcher.batches()).isEqualTo(4);
    }

    @Test
    void fullTenantBacklogIsRejectedWithoutBlocking() throws Exception {
        batcher = new UsageBatcher(ledger, 10, 1, 2);
        var first = batcher.submit("t1", charge("in-flight"));
        assertThat(ledger.entered.await(5, TimeUnit.SECONDS)).isTrue();
        var second = batcher.submit("t1", charge("queued-1"));
        var third = batcher.submit("t1", charge("queued-2"));

        assertThatThrownBy(() -> batcher.submit("t1", charge("overflow")))
                .isInstanceOf(ApiError.class)
                .satisfies(e -> assertThat(((ApiError) e).code()).isEqualTo("USAGE_BACKLOG_FULL"));
        // Other tenants have their own backlog.
        var otherTenant = batcher.submit("t2", charge("other"));
        ledger.release.countDown();
        for (var result : List.of(first, second, third, otherTenant)) {
            assertThat(result.get(5, TimeUnit.SECONDS)).isNotNull();
        }
    }

    @Test
    void failedTransactionFailsEveryChargeInTheBatch() throws Exception {
        batcher = new UsageBatcher(ledger, 10, 1, 100);
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
    }

    @Test
    void submissionsAfterCloseAreRefused() throws Exception {
        batcher = new UsageBatcher(ledger, 10, 1, 100);
        ledger.release.countDown();
        batcher.close();

        assertThatThrownBy(() -> batcher.submit("t1", charge("late")))
                .isInstanceOf(ApiError.class)
                .satisfies(e -> assertThat(((ApiError) e).code()).isEqualTo("SHUTTING_DOWN"));
    }
}
