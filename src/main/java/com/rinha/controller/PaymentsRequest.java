package com.rinha.controller;

import java.time.Instant;

public record PaymentsRequest(
        String correlationId,
        double amount,
        Instant requestedAt
) {
}
