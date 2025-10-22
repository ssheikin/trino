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
package io.trino.plugin.hive;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.configuration.ConfigPropertyMetadata;
import io.airlift.json.JsonModule;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.manager.FileSystemModule;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.base.CatalogNameModule;
import io.trino.plugin.base.TypeDeserializerModule;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorAccessControl;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorCacheMetadata;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSinkProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitManager;
import io.trino.plugin.base.classloader.ClassLoaderSafeNodePartitioningProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeSystemTable;
import io.trino.plugin.base.config.ConfigUtils;
import io.trino.plugin.base.jmx.ConnectorObjectNameGeneratorModule;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.hive.fs.CachingDirectoryListerModule;
import io.trino.plugin.hive.fs.DirectoryLister;
import io.trino.plugin.hive.metastore.HiveMetastoreModule;
import io.trino.plugin.hive.procedure.HiveProcedureModule;
import io.trino.plugin.hive.security.HiveSecurityModule;
import io.trino.plugin.hive.security.SystemTableAwareAccessControl;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.NodeVersion;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.PageSorter;
import io.trino.spi.VersionEmbedder;
import io.trino.spi.cache.ConnectorCacheMetadata;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.MetadataProvider;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.security.LocationAccessControl;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;

public class HiveConnectorFactory
        implements ConnectorFactory
{
    private static final Module DEFAULT_ADDITIONAL_MODULE = EMPTY_MODULE;
    private static final Optional<HiveMetastore> DEFAULT_METASTORE = Optional.empty();
    private static final Optional<TrinoFileSystemFactory> DEFAULT_FILESYSTEM_FACTORY = Optional.empty();
    private static final Optional<DirectoryLister> DEFAULT_DIRECTORY_LISTENER = Optional.empty();

    @Override
    public String getName()
    {
        return "hive";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        checkStrictSpiVersionMatch(context, this);
        return createConnector(catalogName, config, context, DEFAULT_ADDITIONAL_MODULE, DEFAULT_METASTORE, DEFAULT_FILESYSTEM_FACTORY, DEFAULT_DIRECTORY_LISTENER);
    }

    @Override
    public Set<String> getSecuritySensitivePropertyNames(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        ClassLoader classLoader = HiveConnectorFactory.class.getClassLoader();
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = createBootstrap(catalogName, config, context, DEFAULT_ADDITIONAL_MODULE, DEFAULT_METASTORE, DEFAULT_FILESYSTEM_FACTORY, DEFAULT_DIRECTORY_LISTENER, true);

            Set<ConfigPropertyMetadata> usedProperties = app.configure();

            return ConfigUtils.getSecuritySensitivePropertyNames(config, usedProperties);
        }
    }

    public static Connector createConnector(
            String catalogName,
            Map<String, String> config,
            ConnectorContext context,
            Module module,
            Optional<HiveMetastore> metastore,
            Optional<TrinoFileSystemFactory> fileSystemFactory,
            Optional<DirectoryLister> directoryLister)
    {
        ClassLoader classLoader = HiveConnectorFactory.class.getClassLoader();
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = createBootstrap(catalogName, config, context, module, metastore, fileSystemFactory, directoryLister, false);

            Injector injector = app.initialize();

            LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
            HiveTransactionManager transactionManager = injector.getInstance(HiveTransactionManager.class);
            ConnectorSplitManager splitManager = injector.getInstance(ConnectorSplitManager.class);
            ConnectorCacheMetadata cacheMetadata = injector.getInstance(ConnectorCacheMetadata.class);
            ConnectorPageSourceProvider connectorPageSource = injector.getInstance(ConnectorPageSourceProvider.class);
            ConnectorPageSinkProvider pageSinkProvider = injector.getInstance(ConnectorPageSinkProvider.class);
            ConnectorNodePartitioningProvider connectorDistributionProvider = injector.getInstance(ConnectorNodePartitioningProvider.class);
            Set<SystemTable> systemTables = injector.getInstance(new Key<>() {});
            Set<SessionPropertiesProvider> sessionPropertiesProviders = injector.getInstance(new Key<>() {});
            HiveTableProperties hiveTableProperties = injector.getInstance(HiveTableProperties.class);
            HiveViewProperties hiveViewProperties = injector.getInstance(HiveViewProperties.class);
            HiveColumnProperties hiveColumnProperties = injector.getInstance(HiveColumnProperties.class);
            HiveAnalyzeProperties hiveAnalyzeProperties = injector.getInstance(HiveAnalyzeProperties.class);
            HiveMaterializedViewPropertiesProvider hiveMaterializedViewPropertiesProvider = injector.getInstance(HiveMaterializedViewPropertiesProvider.class);
            Set<Procedure> procedures = injector.getInstance(new Key<>() {});
            Set<TableProcedureMetadata> tableProcedures = injector.getInstance(new Key<>() {});
            Set<SystemTableProvider> systemTableProviders = injector.getInstance(new Key<>() {});
            Optional<ConnectorAccessControl> hiveAccessControl = injector.getInstance(new Key<Optional<ConnectorAccessControl>>() {})
                    .map(accessControl -> new SystemTableAwareAccessControl(accessControl, systemTableProviders))
                    .map(accessControl -> new ClassLoaderSafeConnectorAccessControl(accessControl, classLoader));

            return new HiveConnector(
                    injector,
                    lifeCycleManager,
                    transactionManager,
                    new ClassLoaderSafeConnectorSplitManager(splitManager, classLoader),
                    new ClassLoaderSafeConnectorCacheMetadata(cacheMetadata, classLoader),
                    new ClassLoaderSafeConnectorPageSourceProvider(connectorPageSource, classLoader),
                    new ClassLoaderSafeConnectorPageSinkProvider(pageSinkProvider, classLoader),
                    new ClassLoaderSafeNodePartitioningProvider(connectorDistributionProvider, classLoader),
                    systemTables.stream().map(systemTable -> new ClassLoaderSafeSystemTable(systemTable, classLoader)).collect(toImmutableSet()),
                    procedures,
                    tableProcedures,
                    sessionPropertiesProviders,
                    HiveSchemaProperties.SCHEMA_PROPERTIES,
                    hiveTableProperties.getTableProperties(),
                    hiveViewProperties.getViewProperties(),
                    hiveColumnProperties.getColumnProperties(),
                    hiveAnalyzeProperties.getAnalyzeProperties(),
                    hiveMaterializedViewPropertiesProvider.getMaterializedViewProperties(),
                    hiveAccessControl,
                    injector.getInstance(new Key<>() {}),
                    injector.getInstance(FunctionProvider.class),
                    injector.getInstance(HiveConfig.class).isSingleStatementWritesOnly(),
                    classLoader);
        }
    }

    @VisibleForTesting
    public static Bootstrap createBootstrap(
            String catalogName,
            Map<String, String> config,
            ConnectorContext context,
            Module module,
            Optional<HiveMetastore> metastore,
            Optional<TrinoFileSystemFactory> fileSystemFactory,
            Optional<DirectoryLister> directoryLister,
            boolean quietBootstrap)
    {
        Bootstrap app = new Bootstrap(
                "io.trino.bootstrap.catalog." + catalogName,
                new CatalogNameModule(catalogName),
                new MBeanModule(),
                new ConnectorObjectNameGeneratorModule("io.trino.plugin.hive", "trino.plugin.hive"),
                new JsonModule(),
                new TypeDeserializerModule(context.getTypeManager()),
                new HiveModule(),
                new CachingDirectoryListerModule(directoryLister),
                new HiveMetastoreModule(metastore, true),
                new HiveSecurityModule(),
                fileSystemFactory
                        .map(factory -> (Module) binder -> binder.bind(TrinoFileSystemFactory.class).toInstance(factory))
                        .orElseGet(() -> new FileSystemModule(catalogName, context.getNodeManager(), context.getCurrentNode().isCoordinator(), context.getOpenTelemetry(), false, quietBootstrap)),
                new HiveProcedureModule(),
                new MBeanServerModule(),
                binder -> {
                    binder.bind(OpenTelemetry.class).toInstance(context.getOpenTelemetry());
                    binder.bind(Tracer.class).toInstance(context.getTracer());
                    binder.bind(NodeVersion.class).toInstance(new NodeVersion(context.getCurrentNode().getVersion()));
                    binder.bind(Node.class).toInstance(context.getCurrentNode());
                    binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                    binder.bind(VersionEmbedder.class).toInstance(context.getVersionEmbedder());
                    binder.bind(MetadataProvider.class).toInstance(context.getMetadataProvider());
                    binder.bind(PageIndexerFactory.class).toInstance(context.getPageIndexerFactory());
                    binder.bind(PageSorter.class).toInstance(context.getPageSorter());
                    binder.bind(CatalogName.class).toInstance(new CatalogName(catalogName));
                    binder.bind(LocationAccessControl.class).toInstance(context.getLocationAccessControl());
                },
                binder -> newSetBinder(binder, EventListener.class),
                binder -> newSetBinder(binder, SessionPropertiesProvider.class).addBinding().to(HiveSessionProperties.class).in(Scopes.SINGLETON),
                module);

        if (quietBootstrap) {
            app.quiet()
                    .skipErrorReporting();
        }

        return app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config);
    }
}
