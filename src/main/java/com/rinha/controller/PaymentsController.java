package com.rinha.controller;

import com.rinha.PaymentService;
import io.quarkus.virtual.threads.VirtualThreads;
import io.smallrye.common.annotation.RunOnVirtualThread;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import org.jboss.resteasy.reactive.ResponseStatus;

import java.time.Instant;
import java.util.concurrent.ExecutorService;

@Path("/")
public class PaymentsController {

    @Inject
    @VirtualThreads
    ExecutorService vThreads;

    @Inject
    PaymentService paymentService;

    @POST
    @Path("/payments")
    @RunOnVirtualThread
    @ResponseStatus(202)
    public void processPayment(PaymentsRequest paymentsRequest) {
        vThreads.submit(() -> paymentService.processPayment(paymentsRequest));
    }


    @GET
    @Path("/payments-summary")
    @Produces(MediaType.APPLICATION_JSON)
    public Uni<PaymentsSummary> getPaymentsSummary(
            @QueryParam("from") String from,
            @QueryParam("to") String to) {

        Instant fromInstant = from == null ? Instant.MIN : Instant.parse(from);
        Instant toInstant = to == null ? Instant.MAX : Instant.parse(to);

        return paymentService.getPaymentsSummary(fromInstant, toInstant);
    }
}