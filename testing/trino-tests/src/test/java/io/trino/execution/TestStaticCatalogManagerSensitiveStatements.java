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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Key;
import io.trino.connector.CatalogManagerConfig.CatalogMangerKind;
import io.trino.connector.ConnectorServicesProvider;
import io.trino.plugin.hive.HivePlugin;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static org.assertj.core.api.Assertions.assertThat;

final class TestStaticCatalogManagerSensitiveStatements
{
    @Test
    public void testShowCreateCatalog(@TempDir Path configurationPath)
            throws Exception
    {
        Path catalog = Files.createFile(Paths.get(configurationPath.resolve("hive.properties").toUri()));
        Files.write(catalog, ImmutableList.of("connector.name=hive", "hive.metastore=file", "hive.metastore.catalog.dir=s3://hive/metastore"));

        try (QueryRunner queryRunner = new StandaloneQueryRunner(
                TEST_SESSION,
                builder -> builder
                        .setProperties(ImmutableMap.of(
                                "catalog.config-dir", configurationPath.toAbsolutePath().toString()))
                        .setCatalogMangerKind(CatalogMangerKind.STATIC))) {
            queryRunner.installPlugin(new HivePlugin());
            queryRunner.getCoordinator().getInstance(Key.get(ConnectorServicesProvider.class)).loadInitialCatalogs();

            MaterializedResult result = queryRunner.execute("SHOW CREATE CATALOG hive");
            assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo("""
                    CREATE CATALOG hive USING hive
                    WITH (
                       "hive.metastore" = '***',
                       "hive.metastore.catalog.dir" = '***'
                    )""");
        }
    }
}
