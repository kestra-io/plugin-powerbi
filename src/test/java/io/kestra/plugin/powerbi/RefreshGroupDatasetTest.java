package io.kestra.plugin.powerbi;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
@WireMockTest
class RefreshGroupDatasetTest {
    private static final String LOGIN_PATH = "/login/tenant/oauth2/token";
    private static final String REFRESHES_PATH = "/api/v1.0/myorg/groups/group/datasets/dataset/refreshes";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void refreshGroupDataset(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        // Configure the task to send request to WireMock.
        AbstractPowerBi.LOGIN_URL = "http://localhost:" + wmRuntimeInfo.getHttpPort() + "/login";
        AbstractPowerBi.API_URL = "http://localhost:" + wmRuntimeInfo.getHttpPort() + "/api";

        RunContext runContext = runContextFactory.of();
        var task = RefreshGroupDataset.builder()
            .tenantId("tenant")
            .clientId("client")
            .clientSecret("secret")
            .groupId(Property.ofValue("group"))
            .datasetId(Property.ofValue("dataset"))
            .wait(Property.ofValue(true))
            .build();

        var output = task.run(runContext);
        assertThat(output, notNullValue());
        assertThat(output.getRequestId(), is("954e322b-29d4-7023-6421-dab1284343b6"));
        assertThat(output.getStatus(), is("Completed"));
    }

    @Test
    void fetchesNewTokenOnEachRunAndReusesItWhilePolling(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        AbstractPowerBi.LOGIN_URL = "http://localhost:" + wmRuntimeInfo.getHttpPort() + "/login";
        AbstractPowerBi.API_URL = "http://localhost:" + wmRuntimeInfo.getHttpPort() + "/api";

        var wireMock = wmRuntimeInfo.getWireMock();
        wireMock.register(
            post(urlEqualTo(LOGIN_PATH)).atPriority(1)
                .inScenario("login").whenScenarioStateIs(STARTED)
                .willReturn(okJson("{\"access_token\": \"token-first-run\"}"))
                .willSetStateTo("second-run")
        );
        wireMock.register(
            post(urlEqualTo(LOGIN_PATH)).atPriority(1)
                .inScenario("login").whenScenarioStateIs("second-run")
                .willReturn(okJson("{\"access_token\": \"token-second-run\"}"))
        );

        // each run sees the refresh as Unknown first, then Completed, so it polls twice
        wireMock.register(
            get(urlEqualTo(REFRESHES_PATH)).atPriority(1)
                .inScenario("polling").whenScenarioStateIs(STARTED)
                .willReturn(okJson(refreshes("Unknown")))
                .willSetStateTo("completed")
        );
        wireMock.register(
            get(urlEqualTo(REFRESHES_PATH)).atPriority(1)
                .inScenario("polling").whenScenarioStateIs("completed")
                .willReturn(okJson(refreshes("Completed")))
                .willSetStateTo(STARTED)
        );

        var task = RefreshGroupDataset.builder()
            .tenantId("tenant")
            .clientId("client")
            .clientSecret("secret")
            .groupId(Property.ofValue("group"))
            .datasetId(Property.ofValue("dataset"))
            .wait(Property.ofValue(true))
            .pollDuration(Property.ofValue(Duration.ofMillis(50)))
            .build();

        var first = task.run(runContextFactory.of());
        var second = task.run(runContextFactory.of());

        assertThat(first.getStatus(), is("Completed"));
        assertThat(second.getStatus(), is("Completed"));

        wireMock.verifyThat(2, postRequestedFor(urlEqualTo(LOGIN_PATH)));
        for (var token : List.of("token-first-run", "token-second-run")) {
            var authorization = equalTo("Bearer " + token);
            wireMock.verifyThat(1, postRequestedFor(urlEqualTo(REFRESHES_PATH)).withHeader("Authorization", authorization));
            wireMock.verifyThat(2, getRequestedFor(urlEqualTo(REFRESHES_PATH)).withHeader("Authorization", authorization));
        }
    }

    private static String refreshes(String status) {
        return """
            {"value": [{"requestId": "954e322b-29d4-7023-6421-dab1284343b6", "refreshType": "OnDemand", "status": "%s"}]}
            """.formatted(status);
    }
}