package io.kestra.plugin.powerbi;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.is;

@KestraTest
@WireMockTest
class RefreshGroupDatasetTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void test_something_with_wiremock(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        // The static DSL will be automatically configured for you
//        stubFor(get("/static-dsl").willReturn(ok()));

        // Instance DSL can be obtained from the runtime info parameter
//        WireMock wireMock = wmRuntimeInfo.getWireMock();
//        wireMock.register(get("/instance-dsl").willReturn(ok()));

        // Info such as port numbers is also available
        int port = wmRuntimeInfo.getHttpPort();
        AbstractPowerBi.LOGIN_URL = "http://localhost:" + port + "/login";
        AbstractPowerBi.API_URL = "http://localhost:" + port + "/api";

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