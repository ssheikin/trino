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

import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static io.trino.client.StatementClientFactory.newStatementClient;
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
        return newStatementClient(trinoUri, SOURCE, session, query, Optional.empty());
    }

    private static Properties toProperties(Map<String, Object> map)
    {
        Properties properties = new Properties();
        properties.putAll(requireNonNull(map, "map is null"));
        return properties;
    }
}
