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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.plugin.jdbc.credential.CredentialConfig;
import io.trino.plugin.jdbc.credential.CredentialPropertiesProvider;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Map;
import java.util.Optional;

import static io.trino.client.uri.PropertyName.PASSWORD;
import static io.trino.client.uri.PropertyName.USER;

public class StaticCredentialPropertiesProvider
        implements CredentialPropertiesProvider
{
    private final Optional<String> connectionUser;
    private final Optional<String> connectionPassword;

    @Inject
    public StaticCredentialPropertiesProvider(CredentialConfig credentialConfig)
    {
        this.connectionUser = credentialConfig.getConnectionUser();
        this.connectionPassword = credentialConfig.getConnectionPassword();
    }

    @Override
    public Map<String, Object> getCredentialProperties(ConnectorIdentity identity)
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        connectionUser.ifPresent(value -> properties.put(USER.toString(), value));
        connectionPassword.ifPresent(value -> properties.put(PASSWORD.toString(), value));
        return properties.buildOrThrow();
    }
}
