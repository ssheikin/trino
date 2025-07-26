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
package io.trino.plugin.iceberg.catalog.glue.v2;

import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergScheduledMvRefreshConfig;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.glue.IcebergGlueCatalogConfig;
import io.trino.plugin.iceberg.catalog.glue.TestTrinoGlueCatalog;
import io.trino.spi.NoopWorkScheduler;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.type.TestingTypeManager;
import software.amazon.awssdk.services.glue.GlueClient;

import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.iceberg.IcebergTestUtils.FILE_IO_FACTORY;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;

public class TestTrinoGlueCatalogV2
        extends TestTrinoGlueCatalog
{
    @Override
    protected TrinoCatalog createTrinoCatalog(boolean useUniqueTableLocations)
    {
        return createGlueTrinoCatalog(useUniqueTableLocations, false);
    }

    private TrinoCatalog createGlueTrinoCatalog(boolean useUniqueTableLocations, boolean useSystemSecurity)
    {
        GlueClient glueClient = GlueClient.create();
        IcebergGlueCatalogConfig catalogConfig = new IcebergGlueCatalogConfig();
        return new TrinoGlueCatalogV2(
                new CatalogName("catalog_name"),
                new NoopWorkScheduler(),
                HDFS_FILE_SYSTEM_FACTORY,
                FILE_IO_FACTORY,
                new TestingTypeManager(),
                catalogConfig.isCacheTableMetadata(),
                new GlueIcebergTableOperationsProviderV2(
                        HDFS_FILE_SYSTEM_FACTORY,
                        FILE_IO_FACTORY,
                        TESTING_TYPE_MANAGER,
                        catalogConfig,
                        new GlueMetastoreStats(),
                        glueClient),
                "test",
                glueClient,
                new GlueMetastoreStats(),
                useSystemSecurity,
                Optional.empty(),
                useUniqueTableLocations,
                new IcebergConfig().isHideMaterializedViewStorageTable(),
                new IcebergScheduledMvRefreshConfig().isScheduledMaterializedViewRefreshEnabled(),
                directExecutor());
    }
}
