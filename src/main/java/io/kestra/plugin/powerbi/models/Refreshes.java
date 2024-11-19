package io.kestra.plugin.powerbi.models;

import lombok.Value;

import java.util.List;

@Value
public class Refreshes {
    List<Refresh> value;
}
