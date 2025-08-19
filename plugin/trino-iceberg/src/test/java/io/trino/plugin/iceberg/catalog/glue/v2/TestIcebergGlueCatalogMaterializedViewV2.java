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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.SchemaInitializer;
import io.trino.plugin.iceberg.catalog.glue.v1.TestIcebergGlueCatalogMaterializedViewV1;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.nio.file.Files;
import java.util.Map;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;

public class TestIcebergGlueCatalogMaterializedViewV2
        extends TestIcebergGlueCatalogMaterializedViewV1
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.schemaDirectory = Files.createTempDirectory("test_iceberg").toFile();
        schemaDirectory.deleteOnExit();

        DistributedQueryRunner queryRunner = IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.of(
                                "iceberg.catalog.type", "glue_v2",
                                "hive.metastore.glue.default-warehouse-dir", schemaDirectory.getAbsolutePath(),
                                "fs.hadoop.enabled", "true"))
                .setSchemaInitializer(
                        SchemaInitializer.builder()
                                .withClonedTpchTables(ImmutableList.of())
                                .withSchemaName(schemaName)
                                .build())
                .build();
        try {
            queryRunner.createCatalog("iceberg_legacy_mv", "iceberg", Map.of(
                    "iceberg.catalog.type", "glue",
                    "hive.metastore.glue.default-warehouse-dir", schemaDirectory.getAbsolutePath(),
                    "iceberg.materialized-views.hide-storage-table", "false",
                    "fs.hadoop.enabled", "true"));

            queryRunner.installPlugin(createMockConnectorPlugin());
            queryRunner.createCatalog("mock", "mock");
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }
}
