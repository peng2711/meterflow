package dev.peng.meterflow;

import dev.peng.meterflow.Contracts.UsageView;
import dev.peng.meterflow.UsageLedger.Charge;
import dev.peng.meterflow.UsageLedger.Outcome;
import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
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
    private volatile boolean closed;

    UsageBatcher(UsageLedger ledger,
                 @Value("${meterflow.usage.batch-size}") int batchSize,
                 @Value("${meterflow.usage.workers}") int workers,
                 @Value("${meterflow.usage.queue-capacity}") int queueCapacity) {
        if (batchSize < 1 || batchSize > 1000 || workers < 1 || queueCapacity < 1) {
            throw new IllegalArgumentException("batch-size must be 1..1000; workers and queue-capacity must be positive");
        }
        this.ledger = ledger;
        this.batchSize = batchSize;
        this.queueCapacity = queueCapacity;
        AtomicInteger threadNumber = new AtomicInteger();
        this.workers = Executors.newFixedThreadPool(workers,
                r -> new Thread(r, "usage-batch-" + threadNumber.incrementAndGet()));
    }

    CompletableFuture<UsageView> submit(String tenantId, Charge charge) {
        if (closed) {
            throw shuttingDown();
        }
        Pending pending = new Pending(charge, new CompletableFuture<>());
        TenantQueue queue = queues.computeIfAbsent(tenantId, id -> new TenantQueue(id, queueCapacity));
        if (!queue.items.offer(pending)) {
            throw new ApiError(HttpStatus.SERVICE_UNAVAILABLE, "USAGE_BACKLOG_FULL", "该租户待处理上报过多，请稍后重试");
        }
        schedule(queue);
        return pending.result;
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
            stranded.forEach(p -> p.result.completeExceptionally(shuttingDown()));
        }
    }

    private void drain(TenantQueue queue) {
        List<Pending> batch = new ArrayList<>(batchSize);
        queue.items.drainTo(batch, batchSize);
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
        List<Outcome> outcomes;
        try {
            outcomes = ledger.apply(tenantId, batch.stream().map(Pending::charge).toList());
        } catch (Throwable failure) {
            // The transaction rolled back, so none of these charges were recorded; clients may retry.
            batch.forEach(p -> p.result.completeExceptionally(failure));
            return;
        }
        for (int i = 0; i < batch.size(); i++) {
            Outcome outcome = outcomes.get(i);
            if (outcome.error() != null) {
                batch.get(i).result.completeExceptionally(outcome.error());
            } else {
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

    private static ApiError shuttingDown() {
        return new ApiError(HttpStatus.SERVICE_UNAVAILABLE, "SHUTTING_DOWN", "服务正在停止，请稍后重试");
    }

    private record Pending(Charge charge, CompletableFuture<UsageView> result) {}

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
