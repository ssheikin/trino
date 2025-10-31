/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.functions;

import com.google.inject.Injector;
import com.starburstdata.trino.plugin.functions.ai.AiModule;
import com.starburstdata.trino.plugin.functions.io.ResolvingFileSystemModule;
import com.starburstdata.trino.plugin.functions.io.StorageModule;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.configuration.ConfigPropertyMetadata;
import io.airlift.json.JsonModule;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.plugin.base.config.ConfigUtils;
import io.trino.spi.NodeManager;
import io.trino.spi.NodeVersion;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.security.AiModelAccessControl;
import io.trino.spi.security.LocationAccessControl;
import io.trino.spi.type.TypeManager;

import java.util.Map;
import java.util.Set;

import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;

public class FunctionsConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return "starburst_functions";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        checkStrictSpiVersionMatch(context, this);

        Bootstrap app = createBootstrap(catalogName, config, context);

        Injector injector = app
                .initialize();

        return injector.getInstance(Connector.class);
    }

    @Override
    public Set<String> getSecuritySensitivePropertyNames(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        Bootstrap app = createBootstrap(catalogName, config, context);

        Set<ConfigPropertyMetadata> usedProperties = app
                .quiet()
                .skipErrorReporting()
                .configure();

        return ConfigUtils.getSecuritySensitivePropertyNames(config, usedProperties);
    }

    private Bootstrap createBootstrap(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        Bootstrap app = new Bootstrap(
                "io.trino.bootstrap.catalog." + catalogName,
                new AiModule(context.getModelConnectionSpecsLoader()),
                new JsonModule(),
                new StorageModule(),
                new ResolvingFileSystemModule(context.getOpenTelemetry()),
                binder -> {
                    binder.bind(OpenTelemetry.class).toInstance(context.getOpenTelemetry());
                    binder.bind(Tracer.class).toInstance(context.getTracer());
                    binder.bind(CatalogName.class).toInstance(new CatalogName(catalogName));
                    binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                    binder.bind(NodeVersion.class).toInstance(new NodeVersion(context.getCurrentNode().getVersion()));
                    binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                    binder.bind(PageIndexerFactory.class).toInstance(context.getPageIndexerFactory());
                    binder.bind(AiModelAccessControl.class).toInstance(context.getAiModelAccessControl());
                    binder.bind(LocationAccessControl.class).toInstance(context.getLocationAccessControl());
                });
        return app
                .doNotInitializeLogging()
                .loadSecretsPlugins() // starburst-functions-client requires access to secrets.
                .setRequiredConfigurationProperties(config);
    }
}
