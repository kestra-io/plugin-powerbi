package io.kestra.plugin.powerbi;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.Optional;

import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.core.http.ProxyOptions;
import com.azure.identity.ClientSecretCredentialBuilder;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.HttpClientException;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.http.client.configurations.ProxyConfiguration;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;

import io.micronaut.http.MediaType;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractPowerBi extends Task {
    static final String DEFAULT_LOGIN_URL = "https://login.microsoftonline.com";
    static String LOGIN_URL = DEFAULT_LOGIN_URL;

    /** Test seam. The AAD round trip belongs to the SDK now, so tests stub the token rather than the endpoint. */
    static TokenCredential CREDENTIAL;
    static String API_URL = "https://api.powerbi.com/";

    @NotNull
    @NotEmpty
    @Schema(title = "Azure tenant ID")
    @PluginProperty(dynamic = true, group = "main")
    private String tenantId;

    @NotNull
    @NotEmpty
    @Schema(title = "Azure client ID")
    @PluginProperty(dynamic = true, group = "main")
    private String clientId;

    @NotNull
    @NotEmpty
    @Schema(title = "Azure client secret")
    @PluginProperty(dynamic = true, group = "main", secret = true)
    @ToString.Exclude
    private String clientSecret;

    @Schema(
        title = "The http client configuration"
    )
    @PluginProperty(group = "advanced")
    protected HttpConfiguration options;

    /** Power BI issues tokens for this resource, `.default` asks for the app's pre-consented permissions. */
    private static final String SCOPE = "https://analysis.windows.net/powerbi/api/.default";

    @Getter(AccessLevel.NONE)
    private transient TokenCredential credential;

    private String token(RunContext runContext) throws IllegalVariableEvaluationException {
        if (this.credential == null) {
            this.credential = this.credential(runContext);
        }

        // the credential caches and refreshes on its own, so a long `wait` cannot outlive the token
        return this.credential
            .getTokenSync(new TokenRequestContext().addScopes(SCOPE))
            .getToken();
    }

    private TokenCredential credential(RunContext runContext) throws IllegalVariableEvaluationException {
        if (CREDENTIAL != null) {
            return CREDENTIAL;
        }

        ClientSecretCredentialBuilder builder = new ClientSecretCredentialBuilder()
            .tenantId(runContext.render(this.tenantId))
            .clientId(runContext.render(this.clientId))
            .clientSecret(runContext.render(this.clientSecret))
            .authorityHost(LOGIN_URL);

        if (!DEFAULT_LOGIN_URL.equals(LOGIN_URL)) {
            // instance metadata only describes the public cloud, so a custom authority cannot be validated
            builder.disableInstanceDiscovery();
        }

        proxyOptions(runContext).ifPresent(builder::proxyOptions);

        return builder.build();
    }

    /** Carries `options.proxy` onto the token call, which used to go through Kestra's HTTP client. */
    private Optional<ProxyOptions> proxyOptions(RunContext runContext) throws IllegalVariableEvaluationException {
        if (this.options == null || this.options.getProxy() == null) {
            return Optional.empty();
        }

        ProxyConfiguration proxy = this.options.getProxy();
        Proxy.Type type = runContext.render(proxy.getType()).as(Proxy.Type.class).orElse(Proxy.Type.DIRECT);
        Optional<String> address = runContext.render(proxy.getAddress()).as(String.class);
        Optional<Integer> port = runContext.render(proxy.getPort()).as(Integer.class);

        if (type == Proxy.Type.DIRECT || address.isEmpty() || port.isEmpty()) {
            return Optional.empty();
        }

        ProxyOptions proxyOptions = new ProxyOptions(
            type == Proxy.Type.SOCKS ? ProxyOptions.Type.SOCKS5 : ProxyOptions.Type.HTTP,
            new InetSocketAddress(address.get(), port.get())
        );

        Optional<String> username = runContext.render(proxy.getUsername()).as(String.class);
        Optional<String> password = runContext.render(proxy.getPassword()).as(String.class);
        if (username.isPresent() && password.isPresent()) {
            proxyOptions.setCredentials(username.get(), password.get());
        }

        return Optional.of(proxyOptions);
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