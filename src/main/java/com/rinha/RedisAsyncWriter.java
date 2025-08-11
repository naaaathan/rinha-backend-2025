package com.rinha;

import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.sortedset.ReactiveSortedSetCommands;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

@ApplicationScoped
public class RedisAsyncWriter {

    private static final Logger LOG = Logger.getLogger(RedisAsyncWriter.class);
    private static final int CONSUMER_COUNT = 50;

    private final ReactiveSortedSetCommands<String, String> sortedSetCommands;
    private final BlockingQueue<PaymentJob> paymentQueue = new LinkedBlockingQueue<>();
    private final ExecutorService workerExecutor = Executors.newVirtualThreadPerTaskExecutor();

    private record PaymentJob(String key, long timestamp, String member) {}

    @Inject
    public RedisAsyncWriter(ReactiveRedisDataSource redisDataSource) {
        this.sortedSetCommands = redisDataSource.sortedSet(String.class, String.class);
    }

    void onStart(@Observes StartupEvent ev) {
        LOG.debugf("Starting %d Redis writer consumers...", CONSUMER_COUNT);
        for (int i = 0; i < CONSUMER_COUNT; i++) {
            workerExecutor.submit(this::runConsumer);
        }
    }

    public void scheduleSave(String key, long timestamp, String member) {
        if (!paymentQueue.offer(new PaymentJob(key, timestamp, member))) {
            LOG.warn("Payment queue is full. Dropping payment.");
        }
    }

    private void runConsumer() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                var job = paymentQueue.take();
                processJob(job);
            } catch (InterruptedException e) {
                LOG.debug("Consumer thread interrupted. Shutting down.");
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                LOG.error("Error in consumer loop", e);
            }
        }
    }

    private void processJob(PaymentJob job) {
        sortedSetCommands.zadd(job.key(), (double) job.timestamp(), job.member())
                .subscribe().with(
                        success -> {},
                        failure -> LOG.errorf(failure, "Failed to write job for key %s", job.key())
                );
    }

    void onStop(@Observes ShutdownEvent ev) {
        LOG.debug("Shutting down worker executor.");
        workerExecutor.shutdownNow();
    }
}