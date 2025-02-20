package io.kestra.plugin.powerbi.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Value;

import java.time.Instant;

@JsonIgnoreProperties(ignoreUnknown = true)
public record Refresh(String requestId, String refreshType, Instant startTime, Instant endTime, String status,
                      String extendedStatus) {
}
