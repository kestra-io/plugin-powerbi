package io.kestra.plugin.powerbi;

import io.kestra.core.models.property.Property;
import io.kestra.core.utils.Await;
import io.kestra.plugin.powerbi.models.Refresh;
import io.kestra.plugin.powerbi.models.Refreshes;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import org.slf4j.Logger;


import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import jakarta.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwSupplier;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Triggers a refresh for the specified PowerBI dataset from the specified workspace.",
    description = "An [asynchronous refresh](https://docs.microsoft.com/en-us/power-bi/connect-data/asynchronous-refresh) will be triggered."
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

        HttpRequest request = HttpRequest.builder()
            .uri(URI.create(API_URL + "/v1.0/myorg/groups/" + runContext.render(this.groupId).as(String.class).orElseThrow() + "/datasets/" + runContext.render(this.datasetId).as(String.class).orElseThrow() + "/refreshes"))
            .method("POST")
            .build();

            HttpResponse<String> response = this.request(runContext, request, String.class);

            Optional<String> refreshId = response.getHeaders().firstValue("requestId");

            if (refreshId.isEmpty()) {
                throw new IllegalStateException("Invalid request, missing RequestId headers, " +
                    "body '" + response.getBody() + "', header '" + response.getHeaders() + "'");
            }

            logger.info("Refresh created with id '{}'", refreshId.get());

            if (!runContext.render(wait).as(Boolean.class).orElseThrow()) {
                return Output.builder()
                    .requestId(refreshId.get())
                    .build();
            }

            Refresh result = Await.until(
                throwSupplier(() -> {
                    HttpRequest getRequest = HttpRequest.builder()
                        .uri(URI.create(API_URL + "/v1.0/myorg/groups/" + runContext.render(this.groupId).as(String.class).orElse(null) + "/datasets/" + runContext.render(this.datasetId).as(String.class).orElse(null) + "/refreshes"))
                        .method("GET")
                        .build();

                        HttpResponse<Refreshes> refreshResponse = this.request(runContext, getRequest, Refreshes.class);
                        Optional<Refresh> refresh = Optional.ofNullable(refreshResponse.getBody())
                            .map(Refreshes::value)
                            .stream()
                            .flatMap(List::stream)
                            .filter(r -> r.requestId().equals(refreshId.get()))
                            .findFirst();

                        if (refresh.isEmpty()) {
                            throw new IllegalStateException("Unable to find refresh '" + refreshId.get() + "'");
                        }

                        if (logger.isTraceEnabled()) {
                            logger.trace("Refresh: {}", refresh.get());
                        }

                        if (refresh.get().status().equals("Unknown")) {
                            return null;
                        }

                        return refresh.get();

                }),
                runContext.render(this.pollDuration).as(Duration.class).orElseThrow(),
                runContext.render(this.waitDuration).as(Duration.class).orElseThrow()
            );

            if (!result.status().toLowerCase(Locale.ROOT).equals("completed")) {
                throw new Exception("Refresh failed with status '" + result.status() + "' with response " + result);
            }

            return Output.builder()
                .requestId(refreshId.get())
                .status(result.status())
                .extendedStatus(result.extendedStatus())
                .refreshType(result.refreshType())
                .startTime(result.startTime())
                .endTime(result.endTime())
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
