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
package io.trino.plugin.iceberg.catalog.glue;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastoreConfig;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreModule;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.plugin.iceberg.catalog.IcebergTableOperations;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.fileio.ForwardingFileIoFactory;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.TypeManager;
import software.amazon.awssdk.services.glue.GlueClient;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TestingGlueIcebergTableOperationsProvider
        implements IcebergTableOperationsProvider
{
    private final TypeManager typeManager;
    private final boolean cacheTableMetadata;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final GlueClient glueClient;
    private final ForwardingFileIoFactory forwardingFileIoFactory;
    private final GlueMetastoreStats stats;

    @Inject
    public TestingGlueIcebergTableOperationsProvider(
            TypeManager typeManager,
            IcebergGlueCatalogConfig catalogConfig,
            TrinoFileSystemFactory fileSystemFactory,
            ForwardingFileIoFactory forwardingFileIoFactory,
            GlueMetastoreStats stats,
            GlueHiveMetastoreConfig glueConfig,
            AwsGlueAsyncAdapterProvider awsGlueAsyncAdapterProvider)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.cacheTableMetadata = catalogConfig.isCacheTableMetadata();
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.forwardingFileIoFactory = forwardingFileIoFactory;
        this.stats = requireNonNull(stats, "stats is null");
        requireNonNull(glueConfig, "glueConfig is null");
        requireNonNull(awsGlueAsyncAdapterProvider, "awsGlueAsyncAdapterProvider is null");
        this.glueClient = awsGlueAsyncAdapterProvider.createAWSGlueAsyncAdapter(GlueMetastoreModule.createGlueClient(glueConfig, ImmutableSet.of()));
    }

    @Override
    public IcebergTableOperations createTableOperations(
            TrinoCatalog catalog,
            ConnectorSession session,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location)
    {
        return new GlueIcebergTableOperations(
                typeManager,
                cacheTableMetadata,
                glueClient,
                stats,
                ((TrinoGlueCatalog) catalog)::getTable,
                forwardingFileIoFactory.create(fileSystemFactory.create(session)),
                session,
                database,
                table,
                owner,
                location);
    }
}
