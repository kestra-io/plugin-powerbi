package io.kestra.plugin.powerbi.models;

import lombok.Value;

import java.time.Instant;

@Value
public class Refresh {
    String requestId;
    String refreshType;
    Instant startTime;
    Instant endTime;
    String status;
    String extendedStatus;
}
