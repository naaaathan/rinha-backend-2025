package com.rinha.controller;

import com.fasterxml.jackson.annotation.JsonProperty;

public record PaymentsSummary(
        @JsonProperty("default") SummaryDetails defaultSummary,
        @JsonProperty("fallback") SummaryDetails fallbackSummary) {
}