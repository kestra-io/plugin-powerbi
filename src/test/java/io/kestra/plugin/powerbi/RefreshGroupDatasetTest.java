package io.kestra.plugin.powerbi;

import java.time.OffsetDateTime;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.azure.core.credential.AccessToken;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;
import reactor.core.publisher.Mono;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
@WireMockTest
class RefreshGroupDatasetTest {
    @Inject
    private RunContextFactory runContextFactory;

    @AfterEach
    void resetCredential() {
        AbstractPowerBi.CREDENTIAL = null;
    }

    @Test
    void refreshGroupDataset(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        // Configure the task to send request to WireMock.
        AbstractPowerBi.API_URL = "http://localhost:" + wmRuntimeInfo.getHttpPort() + "/api";
        AbstractPowerBi.CREDENTIAL = context -> Mono.just(new AccessToken("access_token", OffsetDateTime.now().plusHours(1)));

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

        // the credential is the SDK's job now, but building the Authorization header is still ours
        verify(
            postRequestedFor(urlPathMatching("/api/.*/refreshes"))
                .withHeader("Authorization", equalTo("Bearer access_token"))
        );
    }
}