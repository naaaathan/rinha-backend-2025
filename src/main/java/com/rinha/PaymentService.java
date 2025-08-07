package com.rinha;

import com.rinha.client.DefaultPaymentsClient;
import com.rinha.client.FallbackPaymentsClient;
import com.rinha.controller.PaymentsRequest;
import com.rinha.controller.PaymentsSummary;
import com.rinha.controller.SummaryDetails;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.sortedset.ReactiveSortedSetCommands;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.redis.client.RedisAPI;
import io.vertx.mutiny.redis.client.Response;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.faulttolerance.CircuitBreaker;
import org.eclipse.microprofile.faulttolerance.Fallback;
import org.eclipse.microprofile.faulttolerance.Retry;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.List;

@ApplicationScoped
public class PaymentService {

    private static final Logger LOG = Logger.getLogger(PaymentService.class);

    private static final String DEFAULT_PAYMENTS_KEY = "payments:default";
    private static final String FALLBACK_PAYMENTS_KEY = "payments:fallback";

    private final DefaultPaymentsClient defaultPaymentsClient;
    private final FallbackPaymentsClient fallbackPaymentsClient;
    private final RedisAPI redisAPI;
    private final RedisAsyncWriter asyncWriter;

    private static final String SUMMARY_SCRIPT = """
            -- Explicitly convert arguments to numbers to ensure correct type matching.
            local min_score = tonumber(ARGV[1])
            local max_score = tonumber(ARGV[2])
            
            -- Now, call ZRANGEBYSCORE with numbers instead of strings.
            local members = redis.call('ZRANGEBYSCORE', KEYS[1], min_score, max_score)
            
            local total_requests = 0
            local total_amount = 0.0
            
            for i, member in ipairs(members) do
                total_requests = total_requests + 1
                local separator_pos = string.find(member, '::', 1, true)
                if separator_pos then
                    local amount_str = string.sub(member, 1, separator_pos - 1)
                    total_amount = total_amount + tonumber(amount_str)
                end
            end
            
            return {tostring(total_requests), tostring(total_amount)}
            """;

    @Inject
    public PaymentService(
            @RestClient DefaultPaymentsClient defaultPaymentsClient,
            @RestClient FallbackPaymentsClient fallbackPaymentsClient,
            RedisAsyncWriter asyncWriter,
            RedisAPI redisAPI
    ) {
        this.defaultPaymentsClient = defaultPaymentsClient;
        this.fallbackPaymentsClient = fallbackPaymentsClient;
        this.redisAPI = redisAPI;
        this.asyncWriter = asyncWriter;
    }

    @Retry(maxRetries = 40, jitter = 0)
    public void processPayment(PaymentsRequest paymentsRequest) {
        this.callDefaultPayment(paymentsRequest);
    }

    @Fallback(fallbackMethod = "callFallbackPayment")
    @CircuitBreaker(requestVolumeThreshold = 4, failureRatio = 0.75, delay = 250)
    public void callDefaultPayment(PaymentsRequest paymentsRequest) {
        var processorRequest = createProcessorRequest(paymentsRequest);
        var response = defaultPaymentsClient.sendPayment(processorRequest);
        LOG.debug("Default Response: " + response);
        savePayment(DEFAULT_PAYMENTS_KEY, paymentsRequest);
        LOG.debug("Default payment request processed");
    }

    public void callFallbackPayment(PaymentsRequest paymentsRequest) {
        LOG.debug("Fallback payment request received");
        var processorRequest = createProcessorRequest(paymentsRequest);
        var response = fallbackPaymentsClient.sendPayment(processorRequest);
        LOG.debug("Fallback response: " + response);
        savePayment(FALLBACK_PAYMENTS_KEY, paymentsRequest);
        LOG.debug("Fallback payment request processed");
    }

    private void savePayment(String key, PaymentsRequest paymentsRequest) {
        long timestamp = Instant.now().toEpochMilli();
        var uniqueMember = paymentsRequest.amount() + "::" + paymentsRequest.correlationId();
        asyncWriter.scheduleSave(key, timestamp, uniqueMember);
    }

    public Uni<PaymentsSummary> getPaymentsSummary(Instant from, Instant to) {
        Uni<SummaryDetails> defaultSummaryUni = getSummaryForKeyWithLua(DEFAULT_PAYMENTS_KEY, from, to);
        Uni<SummaryDetails> fallbackSummaryUni = getSummaryForKeyWithLua(FALLBACK_PAYMENTS_KEY, from, to);
        return Uni.combine().all().unis(defaultSummaryUni, fallbackSummaryUni)
                .asTuple()
                .map(tuple -> new PaymentsSummary(tuple.getItem1(), tuple.getItem2()));
    }

    private Uni<SummaryDetails> getSummaryForKeyWithLua(String key, Instant from, Instant to) {
        String minScore = String.valueOf(from.toEpochMilli());
        String maxScore = String.valueOf(to.toEpochMilli());
        List<String> params = List.of(SUMMARY_SCRIPT, "1", key, minScore, maxScore);

        return redisAPI.eval(params)
                .map(response -> {
                    if (response == null || response.size() == 0) {
                        return new SummaryDetails(0L, 0.0);
                    }
                    long totalRequests = response.get(0).toLong();
                    double totalAmount = response.get(1).toDouble();
                    return new SummaryDetails(totalRequests, totalAmount);
                })
                .onFailure().recoverWithItem(t -> {
                    LOG.errorf(t, "Failed to process Lua script for key %s. Recovering with empty summary.", key);
                    return new SummaryDetails(0L, 0.0);
                });
    }

    private PaymentsRequest createProcessorRequest(PaymentsRequest paymentsRequest) {
        return new PaymentsRequest(
                paymentsRequest.correlationId(),
                paymentsRequest.amount(),
                Instant.now()
        );
    }
}