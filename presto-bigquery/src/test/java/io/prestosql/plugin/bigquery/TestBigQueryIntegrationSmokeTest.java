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
package io.prestosql.plugin.bigquery;

import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.AbstractTestIntegrationSmokeTest;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import org.testng.annotations.Test;

import static io.prestosql.plugin.bigquery.BigQueryQueryRunner.BigQuerySqlExecutor;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;

@Test
public class TestBigQueryIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private BigQuerySqlExecutor bigQuerySqlExecutor;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.bigQuerySqlExecutor = new BigQuerySqlExecutor();
        return BigQueryQueryRunner.createQueryRunner(ImmutableMap.of());
    }

    @Override
    public void testDescribeTable()
    {
        MaterializedResult expectedColumns = resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("clerk", "varchar", "", "")
                .row("shippriority", "bigint", "", "")
                .row("comment", "varchar", "", "")
                .build();
        MaterializedResult actualColumns = computeActual("DESCRIBE orders");
        assertEquals(actualColumns, expectedColumns);
    }

    @Test(enabled = false)
    public void testSelectFromHourlyPartitionedTable()
    {
        onBigQuery("DROP TABLE IF EXISTS test.hourly_partitioned");
        onBigQuery("CREATE TABLE test.hourly_partitioned (value INT64, ts TIMESTAMP) PARTITION BY TIMESTAMP_TRUNC(ts, HOUR)");
        onBigQuery("INSERT INTO test.hourly_partitioned (value, ts) VALUES (1000, '2018-01-01 10:00:00')");

        MaterializedResult actualValues = computeActual("SELECT COUNT(1) FROM test.hourly_partitioned");

        assertEquals((long) actualValues.getOnlyValue(), 1L);
    }

    @Test(enabled = false)
    public void testSelectFromYearlyPartitionedTable()
    {
        onBigQuery("DROP TABLE IF EXISTS test.yearly_partitioned");
        onBigQuery("CREATE TABLE test.yearly_partitioned (value INT64, ts TIMESTAMP) PARTITION BY TIMESTAMP_TRUNC(ts, YEAR)");
        onBigQuery("INSERT INTO test.yearly_partitioned (value, ts) VALUES (1000, '2018-01-01 10:00:00')");

        MaterializedResult actualValues = computeActual("SELECT COUNT(1) FROM test.yearly_partitioned");

        assertEquals((long) actualValues.getOnlyValue(), 1L);
    }

    @Test(description = "regression test for https://github.com/prestosql/presto/issues/5618")
    public void testPredicatePushdownPrunnedColumns()
    {
        String tableName = "test.predicate_pushdown_prunned_columns";

        onBigQuery("DROP TABLE IF EXISTS " + tableName);
        onBigQuery("CREATE TABLE " + tableName + " (a INT64, b INT64, c INT64)");
        onBigQuery("INSERT INTO " + tableName + " VALUES (1,2,3)");

        assertQuery(
                "SELECT 1 FROM " + tableName + " WHERE " +
                        "    ((NULL IS NULL) OR a = 100) AND " +
                        "    b = 2",
                "VALUES (1)");
    }

    @Test(description = "regression test for https://github.com/prestosql/presto/issues/5635")
    public void testCountAggregationView()
    {
        String tableName = "test.count_aggregation_table";
        String viewName = "test.count_aggregation_view";

        onBigQuery("DROP TABLE IF EXISTS " + tableName);
        onBigQuery("DROP VIEW IF EXISTS " + viewName);
        onBigQuery("CREATE TABLE " + tableName + " (a INT64, b INT64, c INT64)");
        onBigQuery("INSERT INTO " + tableName + " VALUES (1, 2, 3), (4, 5, 6)");
        onBigQuery("CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName);

        assertQuery(
                "SELECT count(*) FROM " + viewName,
                "VALUES (2)");

        assertQuery(
                "SELECT count(*) FROM " + viewName + " WHERE a = 1",
                "VALUES (1)");

        assertQuery(
                "SELECT count(a) FROM " + viewName + " WHERE b = 2",
                "VALUES (1)");
    }

    /**
     * https://github.com/trinodb/trino/issues/8183
     */
    @Test
    public void testColumnPositionMismatch()
    {
        String tableName = "test.test_column_position_mismatch";

        onBigQuery("DROP TABLE IF EXISTS " + tableName);
        onBigQuery("CREATE TABLE " + tableName + " (c_varchar STRING, c_int INT64, c_date DATE)");
        onBigQuery("INSERT INTO " + tableName + " VALUES ('a', 1, '2021-01-01')");
        // Adding a CAST makes BigQuery return columns in a different order
        assertQuery(
                "SELECT c_varchar, CAST(c_int AS SMALLINT), c_date FROM " + tableName,
                "VALUES ('a', 1, '2021-01-01')");

        onBigQuery("DROP TABLE " + tableName);
    }

    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo("CREATE TABLE bigquery.tpch.orders (\n" +
                        "   orderkey bigint NOT NULL,\n" +
                        "   custkey bigint NOT NULL,\n" +
                        "   orderstatus varchar NOT NULL,\n" +
                        "   totalprice double NOT NULL,\n" +
                        "   orderdate date NOT NULL,\n" +
                        "   orderpriority varchar NOT NULL,\n" +
                        "   clerk varchar NOT NULL,\n" +
                        "   shippriority bigint NOT NULL,\n" +
                        "   comment varchar NOT NULL\n" +
                        ")");
    }

    @Test
    public void testTimeType()
    {
        String tableName = "test.test_time_type";

        onBigQuery("DROP TABLE IF EXISTS " + tableName);
        onBigQuery("CREATE TABLE " + tableName + " (a TIME)");
        onBigQuery("INSERT INTO " + tableName + " VALUES ('01:02:03.123'), ('23:59:59.999')");

        assertThat(query("SELECT a FROM " + tableName))
                .containsAll("VALUES (TIME '01:02:03.123+00:00'), (TIME '23:59:59.999+00:00')");
        assertThat(query("SELECT a FROM " + tableName + " WHERE a = TIME '01:02:03.123+00:00'"))
                .containsAll("VALUES (TIME '01:02:03.123+00:00')");
        assertThat(query("SELECT a FROM " + tableName + " WHERE rand() = 42 OR a = TIME '01:02:03.123+00:00'"))
                .containsAll("VALUES (TIME '01:02:03.123+00:00')");

        onBigQuery("DROP TABLE " + tableName);
    }

    private void onBigQuery(String sql)
    {
        bigQuerySqlExecutor.execute(sql);
    }
}
