/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.filesystem.azure;

import com.azure.core.http.HttpClient;
import com.azure.core.http.okhttp.OkHttpAsyncHttpClientBuilder;
import com.azure.core.tracing.opentelemetry.OpenTelemetryTracingOptions;
import com.azure.core.util.HttpClientOptions;
import com.azure.core.util.TracingOptions;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;
import jakarta.annotation.PreDestroy;
import okhttp3.ConnectionPool;
import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;

import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static io.trino.filesystem.azure.AzureFileSystemConstants.OAUTH2_ACCESS_TOKEN_PASSTHROUGH_CREDENTIAL;
import static java.util.Objects.requireNonNull;

public class AzureFileSystemFactory
        implements TrinoFileSystemFactory
{
    private final AzureAuth auth;
    private final boolean useOauthPassthroughToken;
    private final AzureFileSystemConfig.AuthType authType;
    private final String endpoint;
    private final DataSize readBlockSize;
    private final DataSize writeBlockSize;
    private final int maxWriteConcurrency;
    private final DataSize maxSingleUploadSize;
    private final TracingOptions tracingOptions;
    private final OkHttpClient okHttpClient;
    private final HttpClient httpClient;

    @Inject
    public AzureFileSystemFactory(OpenTelemetry openTelemetry, AzureAuth azureAuth, AzureFileSystemConfig config)
    {
        this(openTelemetry,
                azureAuth,
                config.getAuthType(),
                config.isUseOauthPassthroughToken(),
                config.getEndpoint(),
                config.getReadBlockSize(),
                config.getWriteBlockSize(),
                config.getMaxWriteConcurrency(),
                config.getMaxSingleUploadSize());
    }

    public AzureFileSystemFactory(
            OpenTelemetry openTelemetry,
            AzureAuth azureAuth,
            AzureFileSystemConfig.AuthType authType,
            boolean useOauthPassthroughToken,
            String endpoint,
            DataSize readBlockSize,
            DataSize writeBlockSize,
            int maxWriteConcurrency,
            DataSize maxSingleUploadSize)
    {
        this.auth = requireNonNull(azureAuth, "azureAuth is null");
        this.useOauthPassthroughToken = useOauthPassthroughToken;
        this.authType = requireNonNull(authType, "authType is null");
        this.endpoint = requireNonNull(endpoint, "endpoint is null");
        this.readBlockSize = requireNonNull(readBlockSize, "readBlockSize is null");
        this.writeBlockSize = requireNonNull(writeBlockSize, "writeBlockSize is null");
        checkArgument(maxWriteConcurrency >= 0, "maxWriteConcurrency is negative");
        this.maxWriteConcurrency = maxWriteConcurrency;
        this.maxSingleUploadSize = requireNonNull(maxSingleUploadSize, "maxSingleUploadSize is null");
        this.tracingOptions = new OpenTelemetryTracingOptions().setOpenTelemetry(openTelemetry);

        okHttpClient = new OkHttpClient.Builder().build();
        HttpClientOptions clientOptions = new HttpClientOptions();
        clientOptions.setTracingOptions(tracingOptions);
        httpClient = createAzureHttpClient(okHttpClient, clientOptions);
    }

    @PreDestroy
    public void destroy()
    {
        okHttpClient.dispatcher().executorService().shutdownNow();
        okHttpClient.connectionPool().evictAll();
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        String accessToken = nullToEmpty(identity.getExtraCredentials().get(OAUTH2_ACCESS_TOKEN_PASSTHROUGH_CREDENTIAL));
        return new AzureFileSystem(
                httpClient,
                tracingOptions,
                useOauthPassthroughToken ? new AzureAuthCustomToken(new AzureCustomTokenCredential(accessToken), authType) : auth,
                endpoint,
                readBlockSize,
                writeBlockSize,
                maxWriteConcurrency,
                maxSingleUploadSize);
    }

    public static HttpClient createAzureHttpClient(OkHttpClient okHttpClient, HttpClientOptions clientOptions)
    {
        Integer poolSize = clientOptions.getMaximumConnectionPoolSize();
        // By default, OkHttp uses a maximum idle connection count of 5.
        int maximumConnectionPoolSize = (poolSize != null && poolSize > 0) ? poolSize : 5;
        Dispatcher dispatcher = new Dispatcher();
        dispatcher.setMaxRequests(Runtime.getRuntime().availableProcessors() * 4);
        dispatcher.setMaxRequestsPerHost(Runtime.getRuntime().availableProcessors() * 2);

        return new OkHttpAsyncHttpClientBuilder(okHttpClient)
                .proxy(clientOptions.getProxyOptions())
                .configuration(clientOptions.getConfiguration())
                .connectionTimeout(clientOptions.getConnectTimeout())
                .writeTimeout(clientOptions.getWriteTimeout())
                .readTimeout(clientOptions.getReadTimeout())
                .connectionPool(new ConnectionPool(maximumConnectionPoolSize,
                        clientOptions.getConnectionIdleTimeout().toMillis(), TimeUnit.MILLISECONDS))
                .dispatcher(dispatcher)
                .build();
    }
}
