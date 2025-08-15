package com.rinha;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.common.annotation.Blocking;
import io.vertx.mutiny.redis.client.Command;
import io.vertx.mutiny.redis.client.Redis;
import io.vertx.mutiny.redis.client.RedisConnection;
import io.vertx.mutiny.redis.client.Request;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

@ApplicationScoped
public class RedisAsyncWriter {

    private static final Logger LOG = Logger.getLogger(RedisAsyncWriter.class);

    @ConfigProperty(name = "app.batch.size", defaultValue = "512") // a bit larger helps
    int configuredBatchSize;

    @ConfigProperty(name = "app.consumer.count", defaultValue = "1") // <= 2
    int configuredConsumerCount;

    private static final int QUEUE_CAP = 50_000;
    private static final Duration MAX_WAIT = Duration.ofMillis(2); // time-based flush

    private BlockingQueue<PaymentJob> paymentQueue;
    private ExecutorService workers;

    private record PaymentJob(String key, long ts, String member) {}

    @Inject Redis redisClient;
    private volatile RedisConnection conn;

    @PostConstruct
    void init() {
        paymentQueue = new ArrayBlockingQueue<>(QUEUE_CAP);
        workers = Executors.newVirtualThreadPerTaskExecutor();
    }

    void onStart(@Observes StartupEvent ev) {
        conn = redisClient.connectAndAwait();
        for (int i = 0; i < configuredConsumerCount; i++) workers.submit(this::runConsumer);
    }

    public void scheduleSave(String key, long ts, String member) {
        if (!paymentQueue.offer(new PaymentJob(key, ts, member))) {
            LOG.warn("Payment queue full, dropping");
        }
    }

    private void runConsumer() {
        final int BATCH_SIZE = configuredBatchSize;
        ArrayList<PaymentJob> buf = new ArrayList<>(BATCH_SIZE);
        Map<String, List<PaymentJob>> byKey = new HashMap<>(64);
        final AtomicLong lastFlushNanos = new AtomicLong(System.nanoTime());

        while (!Thread.currentThread().isInterrupted()) {
            try {
                PaymentJob first = paymentQueue.poll(MAX_WAIT.toNanos(), TimeUnit.NANOSECONDS);
                if (first != null) {
                    buf.add(first);
                    paymentQueue.drainTo(buf, BATCH_SIZE - 1);
                }
                if (buf.isEmpty()) {
                    continue;
                }
                byKey.clear();
                for (PaymentJob j : buf) {
                    byKey.computeIfAbsent(j.key(), k -> new ArrayList<>()).add(j);
                }

                List<Request> requests = new ArrayList<>(byKey.size());
                for (var e : byKey.entrySet()) {
                    Request r = Request.cmd(Command.ZADD).arg(e.getKey());
                    for (PaymentJob j : e.getValue()) {
                        r = r.arg(Long.toString(j.ts())).arg(j.member());
                    }
                    requests.add(r);
                }

                conn.batch(requests).subscribe().with(
                        res -> { /* ok */ },
                        t -> LOG.errorf(t, "ZADD batch failed (%d commands)", requests.size())
                );

                buf.clear();
                lastFlushNanos.set(System.nanoTime());
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } catch (Throwable t) {
                LOG.error("Consumer error", t);
                buf.clear();
            }
        }
    }

    void onStop(@Observes ShutdownEvent ev) {
        workers.shutdownNow();
        try { if (conn != null) conn.close(); } catch (Throwable ignore) {}
    }
}
