package io.kestra.plugin.powerbi;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
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
    title = "Refresh a Power BI dataset",
    description = "Starts an asynchronous refresh for a dataset in a workspace. Optionally waits for completion, polling every 5 seconds by default until completion or a 10-minute timeout. Requires a service principal with refresh permissions on the target workspace and dataset."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Refresh a workspace dataset",
            code = """
                id: refresh-powerbi-dataset
                namespace: company.team

                tasks:
                  - id: refresh_sales_model
                    type: io.kestra.plugin.powerbi.RefreshGroupDataset
                    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                    groupId: "9f1c2b8c-12ab-4f3e-9bcd-0af2c5e6d123"
                    datasetId: "5c8b4a1e-7f89-4c33-9dd8-1f23c4567890"
                    wait: true
                    pollDuration: "PT5S"
                    waitDuration: "PT10M"
                """
        )
    }
)
public class RefreshGroupDataset extends AbstractPowerBi implements RunnableTask<RefreshGroupDataset.Output> {
    @Schema(
        title = "Workspace (group) ID",
        description = "GUID of the workspace containing the dataset."
    )
    private Property<String> groupId;

    @Schema(
        title = "Dataset ID",
        description = "GUID of the dataset to refresh."
    )
    private Property<String> datasetId;

    @Schema(
        title = "Wait for refresh completion",
        description = "Defaults to `false`. When `true`, polls the refresh status and fails if the final status is not `Completed`."
    )
    @Builder.Default
    private Property<Boolean> wait = Property.ofValue(false);

    @Schema(
        title = "Polling interval",
        description = "Delay between status checks when `wait` is true; defaults to 5 seconds."
    )
    @NotNull
    @Builder.Default
    private final Property<Duration> pollDuration = Property.ofValue(Duration.ofSeconds(5));

    @Schema(
        title = "Refresh wait timeout",
        description = "Maximum time to wait for completion when `wait` is true; defaults to 10 minutes."
    )
    @NotNull
    @Builder.Default
    private final Property<Duration> waitDuration = Property.ofValue(Duration.ofMinutes(10));

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
            title = "Refresh request ID",
            description = "Always returned, even when `wait` is `false`."
        )
        private String requestId;

        @Schema(
            title = "Refresh status",
            description = "Returned only when `wait` is `true`; task fails if the final status is not `Completed`."
        )
        private String status;

        @Schema(
                title = "Refresh extended status",
                description = "Returned only when `wait` is `true`."
        )
        private String extendedStatus;

        @Schema(
                title = "Refresh type",
                description = "Returned only when `wait` is `true`."
        )
        String refreshType;

        @Schema(
                title = "Refresh start time",
                description = "Returned only when `wait` is `true`."
        )
        Instant startTime;

        @Schema(
                title = "Refresh end time",
                description = "Returned only when `wait` is `true`."
        )
        Instant endTime;
    }
}
