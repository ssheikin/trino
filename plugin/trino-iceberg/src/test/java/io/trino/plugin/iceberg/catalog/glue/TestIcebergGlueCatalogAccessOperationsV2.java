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

import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.plugin.iceberg.IcebergConnector;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.SchemaInitializer;
import io.trino.plugin.iceberg.catalog.glue.v1.TestIcebergGlueCatalogAccessOperationsV1;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

public class TestIcebergGlueCatalogAccessOperationsV2
        extends TestIcebergGlueCatalogAccessOperationsV1
{
    private static final int MAX_PREFIXES_COUNT = 5;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = IcebergQueryRunner.builder(testSchema)
                .addCoordinatorProperty("optimizer.experimental-max-prefetched-information-schema-prefixes", Integer.toString(MAX_PREFIXES_COUNT))
                .addIcebergProperty("iceberg.catalog.type", "glue_v2")
                .addIcebergProperty("hive.metastore.glue.default-warehouse-dir", "local:///glue")
                .setSchemaInitializer(SchemaInitializer.builder().withSchemaName(testSchema).build())
                .build();
        glueStats = ((IcebergConnector) queryRunner.getCoordinator().getConnector("iceberg")).getInjector().getInstance(GlueMetastoreStats.class);
        return queryRunner;
    }
}
