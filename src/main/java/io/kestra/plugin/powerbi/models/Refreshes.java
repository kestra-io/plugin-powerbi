package io.kestra.plugin.powerbi.models;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record Refreshes(List<Refresh> value) {
}
