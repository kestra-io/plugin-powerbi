package io.kestra.plugin.powerbi;

import io.kestra.core.models.property.Property;
import io.kestra.core.utils.Await;
import io.kestra.plugin.powerbi.models.Refresh;
import io.kestra.plugin.powerbi.models.Refreshes;
import io.micronaut.core.type.Argument;
import io.micronaut.http.*;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.http.uri.UriTemplate;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import org.slf4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import jakarta.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwSupplier;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Triggers a refresh for the specified dataset from the specified workspace.",
    description = "An [asynchronous refresh](https://docs.microsoft.com/en-us/power-bi/connect-data/asynchronous-refresh) would be triggered."
)
public class RefreshGroupDataset extends AbstractPowerBi implements RunnableTask<RefreshGroupDataset.Output> {
    @Schema(
        title = "The workspace ID."
    )
    private Property<String> groupId;

    @Schema(
        title = "The dataset ID."
    )
    private Property<String> datasetId;

    @Schema(
        title = "Wait for refresh completion."
    )
    @Builder.Default
    private Property<Boolean> wait = Property.of(false);

    @Schema(
        title = "The duration to wait between the polls."
    )
    @NotNull
    @Builder.Default
    private final Property<Duration> pollDuration = Property.of(Duration.ofSeconds(5));

    @Schema(
        title = "The maximum duration to wait until the refresh completes."
    )
    @NotNull
    @Builder.Default
    private final Property<Duration> waitDuration = Property.of(Duration.ofMinutes(10));

    @Override
    public RefreshGroupDataset.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        HttpResponse<Object> create = this.request(
            runContext,
            HttpRequest.create(
                HttpMethod.POST,
                UriTemplate.of("/v1.0/myorg/groups/{groupId}/datasets/{datasetId}/refreshes")
                    .expand(Map.of(
                        "groupId", runContext.render(this.groupId).as(String.class).orElseThrow(),
                        "datasetId", runContext.render(this.datasetId).as(String.class).orElseThrow()
                    ))
            ),
            Argument.of(Object.class)
        );

        String refreshId = create.getHeaders().get("RequestId");

        if (refreshId == null) {
            throw new IllegalStateException("Invalid request, missing RequestId headers, " +
                "body '" + create.getBody().orElse(null) + "', header '" + create.getHeaders() + "'"
            );
        }

        logger.info("Refresh created with id '{}'", refreshId);

        if (!runContext.render(wait).as(Boolean.class).orElseThrow()) {
            return Output.builder()
                .requestId(refreshId)
                .build();
        }

        Refresh result = Await.until(
            throwSupplier(() -> {
                try {
                    HttpResponse<Refreshes> response = this.request(
                        runContext,
                        HttpRequest.create(
                            HttpMethod.GET,
                            UriTemplate.of("/v1.0/myorg/groups/{groupId}/datasets/{datasetId}/refreshes")
                                .expand(Map.of(
                                    "groupId", runContext.render(this.groupId).as(String.class).orElse(null),
                                    "datasetId", runContext.render(this.datasetId).as(String.class).orElse(null)
                                ))
                        ),
                        Argument.of(Refreshes.class)
                    );

                    Optional<Refresh> refresh = response
                        .getBody()
                        .stream()
                        .flatMap(refreshes -> refreshes.getValue().stream())
                        .filter(r -> r.getRequestId().equals(refreshId))
                        .findFirst();

                    if (refresh.isEmpty()) {
                        throw new IllegalStateException("Unable to find refresh '" + refreshId + "'");
                    }

                    if (logger.isTraceEnabled()) {
                        logger.trace("Refresh: {}", refresh.get());
                    }

                    if (refresh.get().getStatus().equals("Unknown")) {
                        return null;
                    }

                    return refresh.get();
                } catch (HttpClientResponseException e) {
                    throw new Exception(e);
                }
            }),
            runContext.render(this.pollDuration).as(Duration.class).orElseThrow(),
            runContext.render(this.waitDuration).as(Duration.class).orElseThrow()
        );

        if (!result.getStatus().toLowerCase(Locale.ROOT).equals("completed")) {
            throw new Exception("Refresh failed with status '" + result.getStatus() + "' with response " + result);
        }

        return Output.builder()
            .requestId(refreshId)
            .status(result.getStatus())
            .extendedStatus(result.getExtendedStatus())
            .refreshType(result.getRefreshType())
            .startTime(result.getStartTime())
            .endTime(result.getEndTime())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The request ID."
        )
        private String requestId;

        @Schema(
            title = "The refresh status.",
            description = "Only populated if `wait` parameter is set to `true`."
        )
        private String status;

        @Schema(
                title = "The refresh extended status.",
                description = "Only populated if `wait` parameter is set to `true`."
        )
        private String extendedStatus;

        @Schema(
                title = "The refresh type.",
                description = "Only populated if `wait` parameter is set to `true`."
        )
        String refreshType;

        @Schema(
                title = "The refresh start time.",
                description = "Only populated if `wait` parameter is set to `true`."
        )
        Instant startTime;

        @Schema(
                title = "The refresh end time.",
                description = "Only populated if `wait` parameter is set to `true`."
        )
        Instant endTime;
    }
}
