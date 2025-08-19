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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.SchemaInitializer;
import io.trino.plugin.iceberg.catalog.glue.v1.TestIcebergS3AndGlueMetastoreTestV1;
import io.trino.testing.QueryRunner;

import java.net.URI;
import java.util.Map;

import static io.trino.plugin.hive.metastore.glue.TestingGlueHiveMetastore.createTestingGlueHiveMetastore;

public class TestIcebergS3AndGlueMetastoreTest
        extends TestIcebergS3AndGlueMetastoreTestV1
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        metastore = createTestingGlueHiveMetastore(URI.create(schemaPath()), this::closeAfterClass);
        return IcebergQueryRunner.builder()
                .setIcebergProperties(ImmutableMap.<String, String>builder()
                        .put("iceberg.catalog.type", "glue_v2")
                        .put("hive.metastore.glue.default-warehouse-dir", schemaPath())
                        .put("fs.native-s3.enabled", "true")
                        .buildOrThrow())
                .setSchemaInitializer(SchemaInitializer.builder()
                        .withSchemaName(schemaName)
                        .withSchemaProperties(Map.of("location", "'" + schemaPath() + "'"))
                        .build())
                .build();
    }
}
