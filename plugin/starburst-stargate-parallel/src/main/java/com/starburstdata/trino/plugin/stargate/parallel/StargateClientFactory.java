/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate.parallel;

import com.google.inject.Inject;
import io.trino.client.ClientSession;
import io.trino.client.StatementClient;
import io.trino.client.uri.TrinoUri;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.credential.CredentialPropertiesProvider;
import io.trino.spi.security.ConnectorIdentity;
import kotlin.jvm.functions.Function0;
import kotlin.reflect.KClass;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.Request;
import okhttp3.Response;
import okio.Timeout;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static io.trino.client.StatementClientFactory.newStatementClient;
import static io.trino.client.uri.HttpClientFactory.toHttpClientBuilder;
import static java.util.Objects.requireNonNull;

public class StargateClientFactory
{
    private static final String SOURCE = "stargate-parallel";

    private final CredentialPropertiesProvider credentialPropertiesProvider;
    private final String encoding;
    private final String url;

    @Inject
    public StargateClientFactory(CredentialPropertiesProvider credentialPropertiesProvider, StargateParallelConfig parallelConfig, BaseJdbcConfig config)
    {
        this.credentialPropertiesProvider = requireNonNull(credentialPropertiesProvider, "credentialPropertiesProvider is null");
        this.encoding = requireNonNull(parallelConfig, "parallelConfig is null").getEncoding();
        this.url = config.getConnectionUrl().replaceFirst("jdbc:", "");
    }

    public StatementClient createFactory(ConnectorIdentity identity, String query)
    {
        TrinoUri trinoUri = TrinoUri.create(url, toProperties(credentialPropertiesProvider.getCredentialProperties(identity)));
        ClientSession session = trinoUri.toClientSessionBuilder()
                .source(SOURCE)
                .encoding(Optional.of(encoding))
                .build();

        return newStatementClient(toHttpClientBuilder(trinoUri, SOURCE).build(), requestDropping(), session, query, Optional.empty());
    }

    private static Call.Factory requestDropping()
    {
        return _ -> new Call()
        {
            @Override
            public Request request()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Response execute()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void enqueue(Callback callback)
            {
                // Do nothing, this will ensure that SegmentLoader doesn't try to load/ack segments while listing splits
            }

            @Override
            public void cancel()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isExecuted()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isCanceled()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Timeout timeout()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> T tag(KClass<T> kClass)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> T tag(Class<? extends T> aClass)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> T tag(KClass<T> kClass, Function0<? extends T> function0)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> T tag(Class<T> aClass, Function0<? extends T> function0)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Call clone()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static Properties toProperties(Map<String, Object> map)
    {
        Properties properties = new Properties();
        properties.putAll(requireNonNull(map, "map is null"));
        return properties;
    }
}
