/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate;

import java.io.File;
import java.util.Optional;
import java.util.Properties;

public class TrinoUriFactory
{
    private TrinoUriFactory() {}

    public static Properties sslConnectionProperties(StargateConfig connectorConfig, StargateSslConfig sslConfig)
    {
        Properties properties = new Properties();
        if (connectorConfig.isSslEnabled()) {
            setSslProperties(properties, sslConfig);
        }
        return properties;
    }

    private static void setSslProperties(Properties properties, StargateSslConfig sslConfig)
    {
        properties.setProperty("SSL", "true");
        setOptionalProperty(properties, "SSLTrustStorePath", sslConfig.getTruststoreFile().map(File::getAbsolutePath));
        setOptionalProperty(properties, "SSLTrustStorePassword", sslConfig.getTruststorePassword());
        setOptionalProperty(properties, "SSLTrustStoreType", sslConfig.getTruststoreType());
    }

    private static void setOptionalProperty(Properties properties, String propertyKey, Optional<String> maybeValue)
    {
        maybeValue.ifPresent(value -> properties.setProperty(propertyKey, value));
    }
}
