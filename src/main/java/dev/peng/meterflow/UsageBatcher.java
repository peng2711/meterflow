package dev.peng.meterflow;

import dev.peng.meterflow.Contracts.UsageView;
import dev.peng.meterflow.UsageLedger.Charge;
import dev.peng.meterflow.UsageLedger.Outcome;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

/**
 * Group commit per tenant. A durable commit costs far more than the SQL inside it, and one tenant's
 * commits are serialized by its row lock, so charges that arrive while a batch is committing are
 * queued and applied together in the next transaction. There is no wait window: an idle tenant's
 * charge starts a batch of one immediately.
 *
 * <p>Correctness does not depend on this class: {@link UsageLedger} locks the tenant row, so two
 * batches for the same tenant (for example on two instances) are still applied one after another.
 */
@Component
class UsageBatcher {
    private final UsageLedger ledger;
    private final int batchSize;
    private final int queueCapacity;
    private final ExecutorService workers;
    private final ConcurrentHashMap<String, TenantQueue> queues = new ConcurrentHashMap<>();
    private final AtomicLong batches = new AtomicLong();
    private final AtomicInteger pending = new AtomicInteger();
    private final MeterRegistry meters;
    private final DistributionSummary batchSizes;
    private final Timer batchDuration;
    private final Timer queueWait;
    private volatile boolean closed;

    UsageBatcher(UsageLedger ledger,
                 @Value("${meterflow.usage.batch-size}") int batchSize,
                 @Value("${meterflow.usage.workers}") int workers,
                 @Value("${meterflow.usage.queue-capacity}") int queueCapacity,
                 MeterRegistry meters) {
        if (batchSize < 1 || batchSize > 1000 || workers < 1 || queueCapacity < 1) {
            throw new IllegalArgumentException("batch-size must be 1..1000; workers and queue-capacity must be positive");
        }
        this.ledger = ledger;
        this.batchSize = batchSize;
        this.queueCapacity = queueCapacity;
        AtomicInteger threadNumber = new AtomicInteger();
        this.workers = Executors.newFixedThreadPool(workers,
                r -> new Thread(r, "usage-batch-" + threadNumber.incrementAndGet()));
        // No tenant tag: per-tenant series would grow with the number of tenants.
        this.meters = meters;
        this.batchSizes = DistributionSummary.builder("meterflow.usage.batch.size")
                .description("Charges applied by one ledger transaction")
                .serviceLevelObjectives(1, 2, 5, 10, 20, 50, 100)
                .register(meters);
        this.batchDuration = Timer.builder("meterflow.usage.batch.duration")
                .description("Ledger transaction time: tenant lock, SQL and durable commit")
                .publishPercentileHistogram()
                .register(meters);
        this.queueWait = Timer.builder("meterflow.usage.queue.wait")
                .description("Time a charge waits in its tenant queue before its batch starts")
                .publishPercentileHistogram()
                .register(meters);
        Gauge.builder("meterflow.usage.pending", pending, AtomicInteger::get)
                .description("Charges queued and not yet picked up by a batch")
                .register(meters);
    }

    CompletableFuture<UsageView> submit(String tenantId, Charge charge) {
        if (closed) {
            count("shutting_down", 1);
            throw shuttingDown();
        }
        Pending item = new Pending(charge, new CompletableFuture<>(), System.nanoTime());
        TenantQueue queue = queues.computeIfAbsent(tenantId, id -> new TenantQueue(id, queueCapacity));
        // Count before offering: a worker may drain the item before this thread runs again.
        pending.incrementAndGet();
        if (!queue.items.offer(item)) {
            pending.decrementAndGet();
            count("backlog_full", 1);
            throw new ApiError(HttpStatus.SERVICE_UNAVAILABLE, "USAGE_BACKLOG_FULL", "该租户待处理上报过多，请稍后重试");
        }
        schedule(queue);
        return item.result;
    }

    /** Number of committed or failed ledger transactions, for tests and diagnostics. */
    long batches() {
        return batches.get();
    }

    private void schedule(TenantQueue queue) {
        if (!queue.scheduled.compareAndSet(false, true)) {
            return;
        }
        try {
            workers.execute(() -> drain(queue));
        } catch (RejectedExecutionException e) {
            queue.scheduled.set(false);
            List<Pending> stranded = new ArrayList<>();
            queue.items.drainTo(stranded);
            pending.addAndGet(-stranded.size());
            count("shutting_down", stranded.size());
            stranded.forEach(p -> p.result.completeExceptionally(shuttingDown()));
        }
    }

    private void drain(TenantQueue queue) {
        List<Pending> batch = new ArrayList<>(batchSize);
        queue.items.drainTo(batch, batchSize);
        pending.addAndGet(-batch.size());
        if (!batch.isEmpty()) {
            apply(queue.tenantId, batch);
        }
        queue.scheduled.set(false);
        if (!queue.items.isEmpty()) {
            // Go to the back of the executor queue so one hot tenant cannot starve the others.
            schedule(queue);
        } else {
            // A submitter may still hold this queue; schedule() then drains it, so nothing is lost.
            queues.remove(queue.tenantId, queue);
        }
    }

    private void apply(String tenantId, List<Pending> batch) {
        batches.incrementAndGet();
        long started = System.nanoTime();
        batch.forEach(p -> queueWait.record(started - p.enqueuedNanos, TimeUnit.NANOSECONDS));
        batchSizes.record(batch.size());
        List<Outcome> outcomes;
        try {
            outcomes = ledger.apply(tenantId, batch.stream().map(Pending::charge).toList());
        } catch (Throwable failure) {
            // The transaction rolled back, so none of these charges were recorded; clients may retry.
            batchDuration.record(System.nanoTime() - started, TimeUnit.NANOSECONDS);
            count("failed", batch.size());
            batch.forEach(p -> p.result.completeExceptionally(failure));
            return;
        }
        batchDuration.record(System.nanoTime() - started, TimeUnit.NANOSECONDS);
        for (int i = 0; i < batch.size(); i++) {
            Outcome outcome = outcomes.get(i);
            if (outcome.error() != null) {
                count(outcome.error().code().toLowerCase(Locale.ROOT), 1);
                batch.get(i).result.completeExceptionally(outcome.error());
            } else {
                count(outcome.view().replayed() ? "replayed" : "accepted", 1);
                batch.get(i).result.complete(outcome.view());
            }
        }
    }

    @PreDestroy
    void close() throws InterruptedException {
        closed = true;
        workers.shutdown();
        if (!workers.awaitTermination(30, TimeUnit.SECONDS)) {
            workers.shutdownNow();
        }
    }

    private void count(String outcome, int amount) {
        meters.counter("meterflow.usage.charges", "outcome", outcome).increment(amount);
    }

    private static ApiError shuttingDown() {
        return new ApiError(HttpStatus.SERVICE_UNAVAILABLE, "SHUTTING_DOWN", "服务正在停止，请稍后重试");
    }

    private record Pending(Charge charge, CompletableFuture<UsageView> result, long enqueuedNanos) {}

    private static final class TenantQueue {
        final String tenantId;
        final LinkedBlockingQueue<Pending> items;
        final AtomicBoolean scheduled = new AtomicBoolean();

        TenantQueue(String tenantId, int capacity) {
            this.tenantId = tenantId;
            this.items = new LinkedBlockingQueue<>(capacity);
        }
    }
}
