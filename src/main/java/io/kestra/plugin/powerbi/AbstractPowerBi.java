package io.kestra.plugin.powerbi;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.DefaultRunContext;
import io.kestra.core.runners.RunContext;
import io.micronaut.core.type.Argument;
import io.micronaut.http.*;
import io.micronaut.http.client.DefaultHttpClientConfiguration;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.http.client.netty.DefaultHttpClient;
import io.micronaut.http.client.netty.NettyHttpClientFactory;
import io.micronaut.http.codec.MediaTypeCodecRegistry;
import io.micronaut.http.uri.UriTemplate;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Map;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractPowerBi extends Task  {
    @NotNull
    @NotEmpty
    @Schema(
        title = "Azure tenant ID."
    )
    @PluginProperty(dynamic = true)
    private String tenantId;

    @NotNull
    @NotEmpty
    @Schema(
        title = "Azure client ID."
    )
    @PluginProperty(dynamic = true)
    private String clientId;

    @NotNull
    @NotEmpty
    @Schema(
        title = "Azure client secret."
    )
    @PluginProperty(dynamic = true)
    private String clientSecret;

    @Getter(AccessLevel.NONE)
    private transient String token;

    private static final Duration HTTP_READ_TIMEOUT = Duration.ofSeconds(60);
    private static final NettyHttpClientFactory FACTORY = new NettyHttpClientFactory();

    protected HttpClient client(RunContext runContext, String base) throws IllegalVariableEvaluationException, MalformedURLException, URISyntaxException {
        MediaTypeCodecRegistry mediaTypeCodecRegistry = ((DefaultRunContext)runContext).getApplicationContext().getBean(MediaTypeCodecRegistry.class);

        DefaultHttpClientConfiguration configuration = new DefaultHttpClientConfiguration();
        configuration.setReadTimeout(HTTP_READ_TIMEOUT);

        DefaultHttpClient client = (DefaultHttpClient) FACTORY.createClient(URI.create(base).toURL(), configuration);
        client.setMediaTypeCodecRegistry(mediaTypeCodecRegistry);

        return client;
    }

    private String token(RunContext runContext) throws IllegalVariableEvaluationException, MalformedURLException, URISyntaxException {
        if (this.token != null) {
            return this.token;
        }

        UriTemplate uriTemplate = UriTemplate.of("/{tenantId}/oauth2/token");
        String uri = uriTemplate.expand(Map.of(
            "tenantId", runContext.render(this.tenantId)
        ));

        MutableHttpRequest<String> request = HttpRequest.create(HttpMethod.POST, uri)
            .contentType(MediaType.APPLICATION_FORM_URLENCODED)
            .body("grant_type=client_credentials" +
                "&client_id=" + runContext.render(this.clientId)+
                "&client_secret=" + runContext.render(this.clientSecret)+
                "&resource=https://analysis.windows.net/powerbi/api" +
                "&scope=https://analysis.windows.net/powerbi/api/.default"
            );

        try (HttpClient client = this.client(runContext, "https://login.microsoftonline.com")) {
            HttpResponse<Map<String, String>> exchange = client.toBlocking().exchange(request, Argument.mapOf(String.class, String.class));

            Map<String, String> token = exchange.body();

            if (token == null || !token.containsKey("access_token")) {
                throw new IllegalStateException("Invalid token request with response " + token);
            }
            this.token = token.get("access_token");

            return this.token;
        }
    }

    protected <REQ, RES> HttpResponse<RES> request(RunContext runContext, MutableHttpRequest<REQ> request, Argument<RES> argument) throws HttpClientResponseException {
        try {
            request = request
                .bearerAuth(this.token(runContext))
                .contentType(MediaType.APPLICATION_JSON);

            try (HttpClient client = this.client(runContext, "https://api.powerbi.com/")) {
                return client.toBlocking().exchange(request, argument);
            }
        } catch (HttpClientResponseException e) {
            throw new HttpClientResponseException(
                "Request failed '" + e.getStatus().getCode() + "' and body '" + e.getResponse().getBody(String.class).orElse("null") + "'",
                e.getResponse()
            );
        } catch (IllegalVariableEvaluationException | MalformedURLException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
