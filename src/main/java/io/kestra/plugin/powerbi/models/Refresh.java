package io.kestra.plugin.powerbi.models;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record Refresh(String requestId, String refreshType, Instant startTime, Instant endTime, String status,
    String extendedStatus) {
}
