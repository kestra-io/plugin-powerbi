package io.kestra.plugin.powerbi.models;

import lombok.Getter;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

import java.time.Instant;

@Jacksonized
@Value
public class Refresh {
    String requestId;
    String id;
    String refreshType;
    Instant startTime;
    String status;
}
