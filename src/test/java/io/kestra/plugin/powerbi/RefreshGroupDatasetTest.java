package io.kestra.plugin.powerbi;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.is;

@MicronautTest
@WireMockTest
class RefreshGroupDatasetTest {
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
                .groupId("group")
                .datasetId("dataset")
                .wait(true)
                .build();

        var output = task.run(runContext);
        assertThat(output, notNullValue());
        assertThat(output.getRequestId(), is("954e322b-29d4-7023-6421-dab1284343b6"));
        assertThat(output.getStatus(), is("Completed"));
    }
}