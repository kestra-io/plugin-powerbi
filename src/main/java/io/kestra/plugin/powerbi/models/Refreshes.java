package io.kestra.plugin.powerbi.models;

import lombok.Getter;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

import java.time.Instant;
import java.util.List;

@Jacksonized
@Value
public class Refreshes {
    List<Refresh> value;
}
