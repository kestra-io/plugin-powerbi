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
        assertThat(output.getRequestId(), is("1344a272-7893-4afa-a4b3-3fb87222fdac"));
        assertThat(output.getStatus(), is("Completed"));
    }
}