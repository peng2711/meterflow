package dev.peng.meterflow;

import dev.peng.meterflow.Contracts.UsageInput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Fires every request id twice from many threads at once and tallies the outcomes. */
final class Burst {
    private Burst() {}

    static Map<String, Long> twice(UsageService usage, String key, int ids, int threads) throws Exception {
        List<String> calls = new ArrayList<>();
        for (int i = 0; i < ids; i++) {
            calls.add("burst-" + i);
            calls.add("burst-" + i);
        }
        Collections.shuffle(calls, new Random(7));
        CountDownLatch start = new CountDownLatch(1);
        var pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<String>> results = new ArrayList<>();
            for (String requestId : calls) {
                results.add(pool.submit(() -> {
                    if (!start.await(5, TimeUnit.SECONDS)) throw new AssertionError("start timeout");
                    try {
                        return usage.record(key, new UsageInput(requestId, "model-a", 1)).replayed()
                                ? "replayed" : "accepted";
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
            return outcomes.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        } finally {
            pool.shutdownNow();
        }
    }
}
