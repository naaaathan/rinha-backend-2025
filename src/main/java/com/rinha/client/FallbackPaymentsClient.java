package com.rinha.client;

import com.rinha.controller.PaymentsRequest;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

@Path("/payments")
@RegisterRestClient
public interface FallbackPaymentsClient {

    @POST
    PaymentResponse sendPayment(PaymentsRequest paymentsRequest);
}
