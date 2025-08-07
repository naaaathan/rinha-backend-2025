package com.rinha;

import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.sortedset.ReactiveSortedSetCommands;
import io.quarkus.redis.datasource.sortedset.ScoredValue;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
@Startup
public class RedisAsyncWriter {

    private static final Logger LOG = Logger.getLogger(RedisAsyncWriter.class);
    private static final int BATCH_SIZE = 500;

    private final ReactiveSortedSetCommands<String, String> sortedSetCommands;
    private final ConcurrentLinkedQueue<PaymentJob> paymentQueue = new ConcurrentLinkedQueue<>();
    private final ExecutorService workerExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledExecutorService managerScheduler = Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().factory());

    private record PaymentJob(String key, long timestamp, String member) {}

    @Inject
    public RedisAsyncWriter(ReactiveRedisDataSource redisDataSource) {
        this.sortedSetCommands = redisDataSource.sortedSet(String.class, String.class);
        managerScheduler.scheduleWithFixedDelay(this::dispatchWork, 100, 10, TimeUnit.MILLISECONDS);
    }

    public void scheduleSave(String key, long timestamp, String member) {
        paymentQueue.add(new PaymentJob(key, timestamp, member));
    }

    private void dispatchWork() {
        if (paymentQueue.isEmpty()) {
            return;
        }

        List<PaymentJob> batch = new ArrayList<>(BATCH_SIZE);
        while (batch.size() < BATCH_SIZE) {
            PaymentJob job = paymentQueue.poll();
            if (job == null) {
                break;
            }
            batch.add(job);
        }

        if (!batch.isEmpty()) {
            workerExecutor.submit(() -> processBatch(batch));
        }
    }


    @SuppressWarnings("unchecked")
    private void processBatch(List<PaymentJob> batch) {
        if (batch.isEmpty()) {
            return;
        }
        var key = batch.getFirst().key();

        ScoredValue<String>[] scoredValues = batch.stream()
                .map(job -> ScoredValue.of(job.member(), (double) job.timestamp()))
                .toArray(ScoredValue[]::new);

        sortedSetCommands.zadd(key, scoredValues)
                .subscribe().with(
                        success -> {},
                        failure -> LOG.errorf(failure, "Failed to batch write %d items to key %s", batch.size(), key)
                );
    }

    void onStop(@Observes ShutdownEvent ev) {
        managerScheduler.shutdown();
        workerExecutor.shutdown();
    }
}