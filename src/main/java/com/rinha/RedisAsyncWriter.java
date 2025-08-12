package com.rinha;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.vertx.mutiny.redis.client.RedisAPI;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@ApplicationScoped
public class RedisAsyncWriter {

    private static final Logger LOG = Logger.getLogger(RedisAsyncWriter.class);

    @ConfigProperty(name = "app.batch.size", defaultValue = "256")
    int configuredBatchSize;

    @ConfigProperty(name = "app.consumer.count", defaultValue = "10")
    int configuredConsumerCount;

    private static int BATCH_SIZE;
    private static int CONSUMER_COUNT;

    @PostConstruct
    void init() {
        BATCH_SIZE = configuredBatchSize;
        CONSUMER_COUNT = configuredConsumerCount;
    }

    private static final int QUEUE_CAP = 50_000;

    private final BlockingQueue<PaymentJob> paymentQueue = new ArrayBlockingQueue<>(QUEUE_CAP);
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();

    private record PaymentJob(String key, long ts, String member) {
    }

    @Inject
    RedisAPI redis;

    void onStart(@Observes StartupEvent ev) {
        for (int i = 0; i < CONSUMER_COUNT; i++) workers.submit(this::runConsumer);
    }

    public void scheduleSave(String key, long ts, String member) {
        if (!paymentQueue.offer(new PaymentJob(key, ts, member))) {
            LOG.warn("Payment queue full, dropping");
        }
    }

    private void runConsumer() {
        var buf = new ArrayList<PaymentJob>(BATCH_SIZE);
        while (!Thread.currentThread().isInterrupted()) {
            try {
                // block for 1 item, then drain up to batch size
                var first = paymentQueue.take();
                buf.add(first);
                paymentQueue.drainTo(buf, BATCH_SIZE - 1);

                // group by key and flush each group as one ZADD
                var byKey = buf.stream().collect(Collectors.groupingBy(PaymentJob::key));
                for (var e : byKey.entrySet()) {
                    var args = new ArrayList<String>(1 + e.getValue().size() * 2);
                    args.add(e.getKey()); // key
                    for (var j : e.getValue()) {
                        args.add(Long.toString(j.ts()));   // score
                        args.add(j.member());              // member
                    }
                    redis.zadd(args).subscribe().with(
                            r -> { /* ok */ },
                            t -> LOG.errorf(t, "ZADD batch failed for key %s", e.getKey())
                    );
                }
                buf.clear();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } catch (Exception ex) {
                LOG.error("Consumer error", ex);
                buf.clear();
            }
        }
    }

    void onStop(@Observes ShutdownEvent ev) {
        workers.shutdownNow();
    }
}