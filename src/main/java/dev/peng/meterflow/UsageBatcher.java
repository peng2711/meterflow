package dev.peng.meterflow;

import dev.peng.meterflow.Contracts.ReservationView;
import dev.peng.meterflow.Contracts.UsageView;
import dev.peng.meterflow.UsageLedger.Charge;
import dev.peng.meterflow.UsageLedger.Commit;
import dev.peng.meterflow.UsageLedger.Operation;
import dev.peng.meterflow.UsageLedger.Outcome;
import dev.peng.meterflow.UsageLedger.Reserve;
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
 * commits are serialized by its row lock, so operations that arrive while a batch is committing are
 * queued and applied together in the next transaction. There is no wait window: an idle tenant's
 * first operation starts a batch of one immediately.
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
                .description("Operations applied by one ledger transaction")
                .serviceLevelObjectives(1, 2, 5, 10, 20, 50, 100)
                .register(meters);
        this.batchDuration = Timer.builder("meterflow.usage.batch.duration")
                .description("Ledger transaction time: tenant lock, SQL and durable commit")
                .publishPercentileHistogram()
                .register(meters);
        this.queueWait = Timer.builder("meterflow.usage.queue.wait")
                .description("Time an operation waits in its tenant queue before its batch starts")
                .publishPercentileHistogram()
                .register(meters);
        Gauge.builder("meterflow.usage.pending", pending, AtomicInteger::get)
                .description("Operations queued and not yet picked up by a batch")
                .register(meters);
    }

    /** Completes with the operation's view, or exceptionally with its {@link ApiError}. */
    CompletableFuture<Object> submit(String tenantId, Operation operation) {
        if (closed) {
            count(operation, "shutting_down", 1);
            throw shuttingDown();
        }
        Pending item = new Pending(operation, new CompletableFuture<>(), System.nanoTime());
        TenantQueue queue = queues.computeIfAbsent(tenantId, id -> new TenantQueue(id, queueCapacity));
        // Count before offering: a worker may drain the item before this thread runs again.
        pending.incrementAndGet();
        if (!queue.items.offer(item)) {
            pending.decrementAndGet();
            count(operation, "backlog_full", 1);
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
            stranded.forEach(p -> count(p.operation, "shutting_down", 1));
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
            outcomes = ledger.apply(tenantId, batch.stream().map(Pending::operation).toList());
        } catch (Throwable failure) {
            // The transaction rolled back, so none of these operations took effect; clients may retry.
            batchDuration.record(System.nanoTime() - started, TimeUnit.NANOSECONDS);
            batch.forEach(p -> count(p.operation, "failed", 1));
            batch.forEach(p -> p.result.completeExceptionally(failure));
            return;
        }
        batchDuration.record(System.nanoTime() - started, TimeUnit.NANOSECONDS);
        for (int i = 0; i < batch.size(); i++) {
            Outcome outcome = outcomes.get(i);
            Pending pending = batch.get(i);
            if (outcome.error() != null) {
                count(pending.operation, outcome.error().code().toLowerCase(Locale.ROOT), 1);
                pending.result.completeExceptionally(outcome.error());
            } else {
                count(pending.operation, replayed(outcome.view()) ? "replayed" : "accepted", 1);
                pending.result.complete(outcome.view());
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

    private void count(Operation operation, String outcome, int amount) {
        meters.counter("meterflow.usage.operations", "type", type(operation), "outcome", outcome).increment(amount);
    }

    private static String type(Operation operation) {
        if (operation instanceof Charge) return "charge";
        if (operation instanceof Reserve) return "reserve";
        if (operation instanceof Commit) return "commit";
        return "release";
    }

    private static boolean replayed(Object view) {
        return view instanceof UsageView usage ? usage.replayed() : ((ReservationView) view).replayed();
    }

    private static ApiError shuttingDown() {
        return new ApiError(HttpStatus.SERVICE_UNAVAILABLE, "SHUTTING_DOWN", "服务正在停止，请稍后重试");
    }

    private record Pending(Operation operation, CompletableFuture<Object> result, long enqueuedNanos) {}

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
