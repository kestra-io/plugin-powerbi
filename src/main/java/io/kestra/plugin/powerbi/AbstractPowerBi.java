package io.kestra.plugin.powerbi;

import com.fasterxml.jackson.core.type.TypeReference;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.client.HttpClientException;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.HttpClientRequestException;
import io.kestra.core.http.client.HttpClientResponseException;
import io.kestra.core.serializers.JacksonMapper;
import io.micronaut.http.MediaType;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractPowerBi extends Task {
    static String LOGIN_URL = "https://login.microsoftonline.com";
    static String API_URL = "https://api.powerbi.com/";

    @NotNull
    @NotEmpty
    @Schema(title = "Azure tenant ID.")
    @PluginProperty(dynamic = true)
    private String tenantId;

    @NotNull
    @NotEmpty
    @Schema(title = "Azure client ID.")
    @PluginProperty(dynamic = true)
    private String clientId;

    @NotNull
    @NotEmpty
    @Schema(title = "Azure client secret.")
    @PluginProperty(dynamic = true)
    private String clientSecret;

    @Schema(
        title = "The http client configuration"
    )
    protected HttpConfiguration options;

    @Getter(AccessLevel.NONE)
    private transient String token;

    private String token(RunContext runContext) throws IllegalVariableEvaluationException {
        if (this.token != null) {
            return this.token;
        }

        URI uri = URI.create(LOGIN_URL + "/" + runContext.render(this.tenantId) + "/oauth2/token");

        HttpRequest request = HttpRequest.builder()
            .uri(uri)
            .method("POST")
            .body(HttpRequest.UrlEncodedRequestBody.builder()
                .content(Map.of(
                    "grant_type", "client_credentials",
                    "client_id", runContext.render(this.clientId),
                    "client_secret", runContext.render(this.clientSecret),
                    "resource", "https://analysis.windows.net/powerbi/api",
                    "scope", "https://analysis.windows.net/powerbi/api/.default"
                ))
                .build())
            .addHeader("Content-Type", "application/x-www-form-urlencoded")
            .build();

        try (HttpClient client = new HttpClient(runContext, options)) {
            HttpResponse<String> exchange = client.request(request, String.class);
            Map<String, String> tokenResp = JacksonMapper.ofJson().readValue(exchange.getBody(), new TypeReference<>() {});

            if (tokenResp == null || !tokenResp.containsKey("access_token")) {
                throw new IllegalStateException("Invalid token request response: " + token);
            }
            this.token = tokenResp.get("access_token");
            return this.token;
        } catch (HttpClientRequestException | HttpClientResponseException  e) {
            throw new RuntimeException("Failed to fetch access token", e);
        } catch (HttpClientException e) {
            throw new RuntimeException("Cient failed",e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected <REQ, RES> HttpResponse<RES> request(RunContext runContext, HttpRequest request, Class<RES> responseType) throws HttpClientException, IllegalVariableEvaluationException {
            request = HttpRequest.builder()
                .uri(request.getUri())
                .method(request.getMethod())
                .body(request.getBody())
                .addHeader("Authorization", "Bearer " + this.token(runContext))
                .addHeader("Content-Type", MediaType.APPLICATION_JSON)
                .build();


            try (HttpClient client = new HttpClient(runContext, options)) {
                return client.request(request, responseType);
            } catch (IOException | IllegalVariableEvaluationException e) {
                throw new RuntimeException(e);
            }

    }
}