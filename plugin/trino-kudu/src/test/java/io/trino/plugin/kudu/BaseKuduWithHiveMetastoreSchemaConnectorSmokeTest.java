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
package io.trino.plugin.kudu;

import com.google.common.collect.ImmutableList;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.apache.trino.kudu.client.KuduClient;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static io.trino.plugin.kudu.KuduTestTable.create;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

public abstract class BaseKuduWithHiveMetastoreSchemaConnectorSmokeTest
        extends BaseKuduConnectorSmokeTest
{
    private TestingKuduServerWithHiveMetastore kuduServerWithHiveMetastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        kuduServerWithHiveMetastore = closeAfterClass(new TestingKuduServerWithHiveMetastore(getKuduServerVersion()));
        return KuduQueryRunnerFactory.builder(kuduServerWithHiveMetastore.getKuduServer())
                .withSchemaEmulationByHiveMetastore()
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Test
    public void testListingOfTableForDefaultSchema()
    {
        // Test methods may run in parallel and create tables in the default schema
        // Assert at least the TPCH tables exist but there may be more
        List<String> rows = new ArrayList<>(computeActual("SHOW TABLES FROM default").getMaterializedRows())
                .stream()
                .map(row -> ((String) row.getField(0)))
                .collect(toUnmodifiableList());
        assertThat(rows).containsAll(
                REQUIRED_TPCH_TABLES.stream()
                        .map(TpchTable::getTableName)
                        .collect(Collectors.toList()));
    }

    @Test
    public void testCreateTableWithInvalidSpecialCharacters()
    {
        assertQueryFails(
                "CREATE TABLE \"table_with_special_character_$\" (ID BIGINT WITH (primary_key=true))",
                ".* when the Hive Metastore integration is enabled, Kudu table names must be a period \\('\\.'\\) separated database and table name identifier pair, each containing only ASCII alphanumeric characters, '_', and '/': default\\.table_with_special_character_\\$");

        assertQueryFails(
                "CREATE TABLE \"table_with_special_character_@\" (ID BIGINT WITH (primary_key=true))",
                ".* when the Hive Metastore integration is enabled, Kudu table names must be a period \\('\\.'\\) separated database and table name identifier pair, each containing only ASCII alphanumeric characters, '_', and '/': default\\.table_with_special_character_@");

        assertQueryFails(
                "CREATE TABLE \"table_with_special_character_%\" (ID BIGINT WITH (primary_key=true))",
                ".* when the Hive Metastore integration is enabled, Kudu table names must be a period \\('\\.'\\) separated database and table name identifier pair, each containing only ASCII alphanumeric characters, '_', and '/': default\\.table_with_special_character_%");

        assertQueryFails(
                "CREATE TABLE \"table_with_special_character_.\" (ID BIGINT WITH (primary_key=true))",
                ".* when the Hive Metastore integration is enabled, Kudu table names must be a period \\('\\.'\\) separated database and table name identifier pair, each containing only ASCII alphanumeric characters, '_', and '/': default\\.table_with_special_character_\\.");
    }

    @Test
    public void testCreateTableWithSupportedSpecialCharacters()
    {
        String tableName = "table_with_special_character_123/" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE \"%s\" AS SELECT '1' as cola".formatted(tableName), 1);
            assertQuery("SELECT * FROM \"%s\"".formatted(tableName), "VALUES '1'");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS \"%s\"".formatted(tableName));
        }
    }

    @Test
    @Override
    public void testCreateSchema()
    {
        assertThatThrownBy(super::testCreateSchema)
                .hasMessage("Creating schema in Kudu connector not allowed if schema emulation is set to HIVE_METASTORE");
    }

    @Test
    @Override
    public void testCreateSchemaWithNonLowercaseOwnerName()
    {
        assertThatThrownBy(super::testCreateSchemaWithNonLowercaseOwnerName)
                .hasMessage("Creating schema in Kudu connector not allowed if schema emulation is set to HIVE_METASTORE");
    }

    @Test
    @Override
    public void testRenameTableAcrossSchemas()
            throws Exception
    {
        String schemaName = "test_schema_" + randomNameSuffix();
        String oldTable = "test_rename_old_" + randomNameSuffix();
        String newTable = schemaName + ".test_rename_new_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + oldTable + " " + getCreateTableDefaultDefinition());

            kuduServerWithHiveMetastore.exposeHiveSchemaOnKudu(schemaName);
            assertUpdate("ALTER TABLE " + oldTable + " RENAME TO " + newTable);

            assertThat(query("SHOW TABLES LIKE '" + oldTable + "'"))
                    .returnsEmptyResult();
            assertThat(query("SELECT a, b FROM " + newTable))
                    .returnsEmptyResult();

            assertUpdate("INSERT INTO " + newTable + " (a, b) VALUES (42, -38.5)", 1);
            assertThat(query("SELECT CAST(a AS bigint), b FROM " + newTable))
                    .matches("VALUES (BIGINT '42', -385e-1)");

            assertUpdate("DROP TABLE " + newTable);
            assertThat(query("SHOW TABLES LIKE '" + newTable + "'"))
                    .returnsEmptyResult();
        }
        finally {
            kuduServerWithHiveMetastore.dropSchemaOnHive(schemaName);
        }
    }

    @Test
    @Override
    public void testDropSchemaCascade()
    {
        String schemaName = "test_drop_schema_cascade_" + randomNameSuffix();
        assertThatThrownBy(() -> assertUpdate("CREATE SCHEMA " + schemaName))
                .hasMessageContaining("Creating schema in Kudu connector not allowed if schema emulation is set to HIVE_METASTORE");
        abort("Cannot test when schema emulation is set to HIVE_METASTORE");
    }

    @Test
    public void testKuduWithHiveMetastoreSchemaEmulationOnNonLowerCaseTableName()
            throws Exception
    {
        KuduClient kuduClient = kuduServerWithHiveMetastore.getKuduClient();
        String schemaName = "test_schema_" + randomNameSuffix();
        kuduServerWithHiveMetastore.exposeHiveSchemaOnKudu(schemaName);
        assertTablesExists(kuduClient, schemaName, "temp");

        List<KuduTestColumn> columns = ImmutableList.of(new KuduTestColumn(BIGINT, "id", 0, true));

        // Create table with non lower cases
        create(kuduClient, schemaName + ".table_name_Random", columns);
        create(kuduClient, schemaName + ".SAMPLE_TABLE", columns);
        assertTablesExists(kuduClient, schemaName, "temp", "table_name_random", "sample_table");

        assertThatThrownBy(() -> create(kuduClient, schemaName + ".table_name_random", columns))
                .hasMessageContaining("table %s.table_name_random already exists".formatted(schemaName));

        assertThatThrownBy(() -> create(kuduClient, schemaName + ".sample_table", columns))
                .hasMessageContaining("table %s.sample_table already exists".formatted(schemaName));
    }

    private void assertTablesExists(KuduClient kuduClient, String schemaName, String... tableName)
            throws Exception
    {
        assertThat(kuduClient.getTablesList().getTablesList().stream()
                .filter(name -> name.startsWith(schemaName))
                .map(name -> name.split("\\.")[1])
                .toList())
                .containsExactlyInAnyOrder(tableName);
    }
}
