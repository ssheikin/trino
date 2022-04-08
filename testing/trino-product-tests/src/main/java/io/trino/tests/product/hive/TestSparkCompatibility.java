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
package io.trino.tests.product.hive;

import io.trino.tempto.ProductTest;
import io.trino.tempto.query.QueryResult;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.util.List;

import static io.trino.tempto.assertions.QueryAssert.Row;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.HIVE_SPARK;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.hive.util.TemporaryHiveTable.randomTableSuffix;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onSpark;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;

public class TestSparkCompatibility
        extends ProductTest
{
    // see spark-defaults.conf
    private static final String TRINO_CATALOG = "hive";

    @Test(groups = {HIVE_SPARK, PROFILE_SPECIFIC_TESTS})
    public void testReadSparkdDateAndTimePartitionName()
    {
        String sparkTableName = "test_trino_reading_spark_date_and_time_type_partitioned_" + randomTableSuffix();
        String trinoTableName = format("%s.default.%s", TRINO_CATALOG, sparkTableName);

        onSpark().executeQuery(format("CREATE TABLE default.%s (value integer) PARTITIONED BY (dt date)", sparkTableName));

        // Spark allows creating partition with time unit
        // Hive denies creating such partitions, but allows reading
        onSpark().executeQuery(format("INSERT INTO %s PARTITION(dt='2022-04-13 00:00:00.000000000') VALUES (1)", sparkTableName));
        onSpark().executeQuery(format("INSERT INTO %s PARTITION(dt='2022-04-13 00:00:00') VALUES (2)", sparkTableName));
        onSpark().executeQuery(format("INSERT INTO %s PARTITION(dt='2022-04-13 00:00') VALUES (3)", sparkTableName));
        onSpark().executeQuery(format("INSERT INTO %s PARTITION(dt='12345-06-07') VALUES (4)", sparkTableName));
        onSpark().executeQuery(format("INSERT INTO %s PARTITION(dt='123-04-05') VALUES (5)", sparkTableName));
        onSpark().executeQuery(format("INSERT INTO %s PARTITION(dt='-0001-01-01') VALUES (6)", sparkTableName));

        assertThat(onTrino().executeQuery("SELECT \"$partition\" FROM " + trinoTableName))
                .containsOnly(List.of(
                        row("dt=2022-04-13 00%3A00%3A00.000000000"),
                        row("dt=2022-04-13 00%3A00%3A00"),
                        row("dt=2022-04-13 00%3A00"),
                        row("dt=12345-06-07"),
                        row("dt=123-04-05"),
                        row("dt=-0001-01-01")));

        // Use date_format function to avoid exception due to java.sql.Date.valueOf() with 5 digit year
        assertThat(onSpark().executeQuery("SELECT value, date_format(dt, 'yyyy-MM-dd') FROM " + sparkTableName))
                .containsOnly(List.of(
                        row(1, "2022-04-13"),
                        row(2, "2022-04-13"),
                        row(3, "2022-04-13"),
                        row(4, null),
                        row(5, null),
                        row(6, null)));

        // Use date_format function to avoid exception due to java.sql.Date.valueOf() with 5 digit year
        assertThat(onHive().executeQuery("SELECT value, date_format(dt, 'yyyy-MM-dd') FROM " + sparkTableName))
                .containsOnly(List.of(
                        row(1, "2022-04-13"),
                        row(2, "2022-04-13"),
                        row(3, "2022-04-13"),
                        row(4, "12345-06-07"),
                        row(5, "0123-04-06"),
                        row(6, "0002-01-03")));

        // Cast to varchar so that we can compare with Spark & Hive easily
        assertThat(onTrino().executeQuery("SELECT value, CAST(dt AS VARCHAR) FROM " + trinoTableName))
                .containsOnly(List.of(
                        row(1, "2022-04-13"),
                        row(2, "2022-04-13"),
                        row(3, "2022-04-13"),
                        row(4, "12345-06-07"),
                        row(5, "0123-04-05"),
                        row(6, "-0001-01-01")));

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @Test(groups = {HIVE_SPARK, PROFILE_SPECIFIC_TESTS}, dataProvider = "unsupportedPartitionDates")
    public void testReadSparkInvalidDatePartitionName(String inputDate, java.sql.Date outputDate)
    {
        String sparkTableName = "test_trino_reading_spark_invalid_date_type_partitioned_" + randomTableSuffix();
        String trinoTableName = format("%s.default.%s", TRINO_CATALOG, sparkTableName);

        onSpark().executeQuery(format("CREATE TABLE default.%s (value integer) PARTITIONED BY (dt date)", sparkTableName));

        // Spark allows creating partition with invalid date format
        // Hive denies creating such partitions, but allows reading
        onSpark().executeQuery(format("INSERT INTO %s PARTITION(dt='%s') VALUES (1)", sparkTableName, inputDate));

        // Hive ignores time unit, and return null for invalid dates
        assertThat(onHive().executeQuery("SELECT value, dt FROM " + sparkTableName))
                .containsOnly(List.of(row(1, outputDate)));

        // Trino throws an exception if the date is invalid format or not a whole round date
        assertQueryFailure(() -> onTrino().executeQuery("SELECT value, dt FROM " + trinoTableName))
                .hasMessageContaining("Invalid partition value");

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @DataProvider
    public static Object[][] unsupportedPartitionDates()
    {
        return new Object[][] {
                {"1965-09-10 23:59:59.999999999", java.sql.Date.valueOf(LocalDate.of(1965, 9, 10))},
                {"1965-09-10 23:59:59", java.sql.Date.valueOf(LocalDate.of(1965, 9, 10))},
                {"1965-09-10 23:59", java.sql.Date.valueOf(LocalDate.of(1965, 9, 10))},
                {"1965-09-10 00", java.sql.Date.valueOf(LocalDate.of(1965, 9, 10))},
                {"2021-02-30", java.sql.Date.valueOf(LocalDate.of(2021, 3, 2))},
                {"1965-09-10 invalid", java.sql.Date.valueOf(LocalDate.of(1965, 9, 10))},
                {"invalid date", null},
        };
    }

    @Test(groups = {HIVE_SPARK, PROFILE_SPECIFIC_TESTS})
    public void testTrinoReadingTableCreatedByNativeSpark()
    {
        // Spark tables can be created using native Spark code or by going through Hive code
        // This tests the native Spark path.
        String baseTableName = "test_trino_reading_spark_native_buckets_" + randomTableSuffix();

        String sparkTableDefinition =
                "CREATE TABLE `default`.`%s` (\n" +
                        "  `a_string` STRING,\n" +
                        "  `a_bigint` BIGINT,\n" +
                        "  `an_integer` INT,\n" +
                        "  `a_real` FLOAT,\n" +
                        "  `a_double` DOUBLE,\n" +
                        "  `a_boolean` BOOLEAN)\n" +
                        "USING ORC\n" +
                        "CLUSTERED BY (a_string)\n" +
                        "INTO 4 BUCKETS\n" +
                        // Hive requires "original" files of transactional tables to conform to the bucketed tables naming pattern
                        // We can disable transactions or add another pattern to BackgroundHiveSplitLoader
                        "TBLPROPERTIES ('transactional'='false')";
        onSpark().executeQuery(format(sparkTableDefinition, baseTableName));

        String values = "VALUES " +
                "('one', 1000000000000000, 1000000000, 10000000.123, 100000000000.123, true)" +
                ", ('two', -1000000000000000, -1000000000, -10000000.123, -100000000000.123, false)" +
                ", ('three', 2000000000000000, 2000000000, 20000000.123, 200000000000.123, true)" +
                ", ('four', -2000000000000000, -2000000000, -20000000.123, -200000000000.123, false)";
        String insert = format("INSERT INTO %s %s", baseTableName, values);
        onSpark().executeQuery(insert);

        Row row1 = row("one", 1000000000000000L, 1000000000, 10000000.123F, 100000000000.123, true);
        Row row2 = row("two", -1000000000000000L, -1000000000, -10000000.123F, -100000000000.123, false);
        Row row3 = row("three", 2000000000000000L, 2000000000, 20000000.123F, 200000000000.123, true);
        Row row4 = row("four", -2000000000000000L, -2000000000, -20000000.123F, -200000000000.123, false);

        String startOfSelect = "SELECT a_string, a_bigint, an_integer, a_real, a_double, a_boolean";
        QueryResult sparkSelect = onSpark().executeQuery(format("%s FROM %s", startOfSelect, baseTableName));
        assertThat(sparkSelect).containsOnly(row1, row2, row3, row4);

        QueryResult trinoSelect = onTrino().executeQuery(format("%s FROM %s", startOfSelect, format("%s.default.%s", TRINO_CATALOG, baseTableName)));
        assertThat(trinoSelect).containsOnly(row1, row2, row3, row4);

        String trinoTableDefinition =
                "CREATE TABLE %s.default.%s (\n" +
                        "   a_string varchar,\n" +
                        "   a_bigint bigint,\n" +
                        "   an_integer integer,\n" +
                        "   a_real real,\n" +
                        "   a_double double,\n" +
                        "   a_boolean boolean\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'ORC'\n" +
                        ")";
        assertThat(onTrino().executeQuery(format("SHOW CREATE TABLE %s.default.%s", TRINO_CATALOG, baseTableName)))
                .containsOnly(row(format(trinoTableDefinition, TRINO_CATALOG, baseTableName)));

        assertQueryFailure(() -> onTrino().executeQuery(format("%s, \"$bucket\" FROM %s", startOfSelect, format("%s.default.%s", TRINO_CATALOG, baseTableName))))
                .hasMessageContaining("Column '$bucket' cannot be resolved");

        onSpark().executeQuery("DROP TABLE " + baseTableName);
    }
}
