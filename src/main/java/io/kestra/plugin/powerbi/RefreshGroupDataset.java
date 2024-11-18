package io.kestra.plugin.powerbi;

import io.kestra.core.utils.Await;
import io.kestra.plugin.powerbi.models.Refresh;
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
import java.util.*;

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
    @PluginProperty(dynamic = true)
    private String groupId;

    @Schema(
        title = "The dataset ID."
    )
    @PluginProperty(dynamic = true)
    private String datasetId;

    @Schema(
        title = "Wait for refresh completion."
    )
    @PluginProperty
    @Builder.Default
    private Boolean wait = false;

    @Schema(
        title = "The duration to wait between the polls."
    )
    @NotNull
    @Builder.Default
    @PluginProperty
    private final Duration pollDuration = Duration.ofSeconds(5);

    @Schema(
        title = "The maximum duration to wait until the refresh completes."
    )
    @NotNull
    @Builder.Default
    @PluginProperty
    private final Duration waitDuration = Duration.ofMinutes(10);

    @Override
    public RefreshGroupDataset.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        HttpResponse<Object> create = this.request(
            runContext,
            HttpRequest.create(
                HttpMethod.POST,
                UriTemplate.of("/v1.0/myorg/groups/{groupId}/datasets/{datasetId}/refreshes")
                    .expand(Map.of(
                        "groupId", runContext.render(this.groupId),
                        "datasetId", runContext.render(this.datasetId)
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

        if (!wait) {
            return Output.builder()
                .requestId(refreshId)
                .build();
        }

        Refresh result = Await.until(
            throwSupplier(() -> {
                try {
                    HttpResponse<List<Refresh>> response = this.request(
                        runContext,
                        HttpRequest.create(
                            HttpMethod.GET,
                            UriTemplate.of("/v1.0/myorg/groups/{groupId}/datasets/{datasetId}/refreshes")
                                .expand(Map.of(
                                    "groupId", runContext.render(this.groupId),
                                    "datasetId", runContext.render(this.datasetId)
                                ))
                        ),
                        Argument.listOf(Refresh.class)
                    );

                    Optional<Refresh> refresh = response
                        .getBody()
                        .stream()
                        .flatMap(Collection::stream)
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
            this.pollDuration,
            this.waitDuration
        );

        if (!result.getStatus().toLowerCase(Locale.ROOT).equals("completed")) {
            throw new Exception("Refresh failed with status '" + result.getStatus() + "' with response " + result);
        }

        return Output.builder()
            .requestId(refreshId)
            .status(result.getStatus())
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
            title = "The request status.",
            description = "Only populated if `wait` parameter is set to `true`."
        )
        private String status;
    }
}
