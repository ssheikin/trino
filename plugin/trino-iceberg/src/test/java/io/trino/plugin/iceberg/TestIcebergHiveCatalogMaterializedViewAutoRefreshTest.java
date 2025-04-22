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
package io.trino.plugin.iceberg;

import io.trino.Session;
import io.trino.spi.NoopWorkScheduler;
import io.trino.spi.WorkScheduler;
import io.trino.spi.WorkScheduler.RefreshSchedule;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.sql.query.QueryAssertions;
import io.trino.sql.tree.ExplainType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergHiveCatalogMaterializedViewAutoRefreshTest
        extends AbstractTestQueryFramework
{
    protected static final String TEST_CATALOG = "iceberg";
    protected static final String TEST_CATALOG_WITHOUT_SCHEDULING = "iceberg_no_scheduling";
    protected final String schemaName = "test_materialized_view_scheduling_" + randomNameSuffix();
    private TestingWorkScheduler workScheduler;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        workScheduler = new TestingWorkScheduler(this::getQueryRunner);

        return createQueryRunner(workScheduler, ImmutableMap.<String, String>builder()
                .putAll(getIcebergCatalogProperties())
                .put("iceberg.scheduled-materialized-view-refresh-enabled", "true")
                .buildOrThrow());
    }

    @BeforeAll
    public void setupClass()
    {
        getQueryRunner().createCatalog(TEST_CATALOG_WITHOUT_SCHEDULING, "iceberg", ImmutableMap.<String, String>builder()
                .putAll(getIcebergCatalogProperties())
                .put("iceberg.scheduled-materialized-view-refresh-enabled", "false")
                .buildOrThrow());
        getQueryRunner().execute(createSchemaSql(TEST_CATALOG, schemaName));
    }

    @AfterAll
    public void tearDownClass()
    {
        getQueryRunner().execute("DROP SCHEMA %s.%s CASCADE".formatted(TEST_CATALOG, schemaName));
    }

    protected DistributedQueryRunner createQueryRunner(WorkScheduler workScheduler, Map<String, String> icebergCatalogProperties)
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setIcebergProperties(icebergCatalogProperties)
                .setAdditionalModule(binder -> newOptionalBinder(binder, WorkScheduler.class).setBinding().toInstance(workScheduler))
                .build();
    }

    protected String createSchemaSql(String catalog, String schemaName)
    {
        return "CREATE SCHEMA %s.%s".formatted(catalog, schemaName);
    }

    protected Map<String, String> getIcebergCatalogProperties()
    {
        return ImmutableMap.of();
    }

    @Test
    public void testScheduledRefreshDisabled()
    {
        String materializedViewName = "test_scheduled_refresh_disabled" + randomNameSuffix();
        String schema = "without_scheduling_" + randomNameSuffix();
        Session withSchedulingDisabled = Session.builder(getSession())
                .setCatalog(TEST_CATALOG_WITHOUT_SCHEDULING)
                .setSchema(schema)
                .build();
        CatalogSchemaTableName catalogMaterializedViewName = new CatalogSchemaTableName(TEST_CATALOG_WITHOUT_SCHEDULING, new SchemaTableName(schema, materializedViewName));
        try {
            assertUpdate(createSchemaSql(TEST_CATALOG_WITHOUT_SCHEDULING, schema));
            assertThat(query(withSchedulingDisabled, "CREATE MATERIALIZED VIEW " + catalogMaterializedViewName + " WITH (refresh_schedule = '0 0 * * *') AS SELECT 1 AS c"))
                    .failure().hasMessageContaining("materialized view property 'refresh_schedule' does not exist");
            assertQuery("SELECT catalog_name FROM system.metadata.materialized_view_properties WHERE property_name = 'refresh_schedule'", "VALUES '" + TEST_CATALOG + "'");
        }
        finally {
            assertUpdate("DROP SCHEMA %s.%s CASCADE".formatted(TEST_CATALOG_WITHOUT_SCHEDULING, schema));
        }
    }

    @Test
    public void testWorkSchedulerNotAvailable()
            throws Exception
    {
        String materializedViewName = "test_work_scheduler_not_available_" + randomNameSuffix();
        String schema = "without_work_scheduling_" + randomNameSuffix();
        Session withoutWorkScheduler = Session.builder(getSession())
                .setCatalog(TEST_CATALOG)
                .setSchema(schema)
                .build();

        CatalogSchemaTableName catalogMaterializedViewName = new CatalogSchemaTableName(TEST_CATALOG, new SchemaTableName(schema, materializedViewName));
        try (DistributedQueryRunner queryRunner = createQueryRunner(new NoopWorkScheduler(), ImmutableMap.<String, String>builder()
                .putAll(getIcebergCatalogProperties())
                .put("iceberg.scheduled-materialized-view-refresh-enabled", "true")
                .buildOrThrow())) {
            QueryAssertions queryAssertions = new QueryAssertions(queryRunner);
            assertUpdate(queryRunner, withoutWorkScheduler, createSchemaSql(TEST_CATALOG, schema));
            try {
                assertThat(queryAssertions.query(withoutWorkScheduler, "CREATE MATERIALIZED VIEW " + catalogMaterializedViewName + " WITH (refresh_schedule = '0 0 * * *') AS SELECT 1 AS c"))
                        .failure().hasMessageContaining("materialized view property 'refresh_schedule' does not exist");
                assertThat(queryAssertions.query(
                        withoutWorkScheduler,
                        "SELECT catalog_name FROM system.metadata.materialized_view_properties WHERE property_name = 'refresh_schedule'"))
                        .result()
                        .isEmpty();
            }
            finally {
                assertUpdate(queryRunner, withoutWorkScheduler, "DROP SCHEMA %s.%s CASCADE".formatted(TEST_CATALOG, schema));
            }
        }
    }

    private static void assertUpdate(DistributedQueryRunner queryRunner, Session withoutWorkScheduler, String sql)
    {
        io.trino.testing.QueryAssertions.assertUpdate(queryRunner, withoutWorkScheduler, sql, OptionalLong.empty(), Optional.empty());
    }

    @Test
    public void testScheduledRefresh()
    {
        CatalogSchemaTableName materializedView = new CatalogSchemaTableName(
                TEST_CATALOG,
                new SchemaTableName(schemaName, "test_scheduled_refresh" + randomNameSuffix()));

        assertUpdate("CREATE MATERIALIZED VIEW " + materializedView + " WITH (refresh_schedule = '0 0 * * *') AS SELECT * FROM tpch.tiny.nation");

        assertThat(computeActual("SHOW CREATE MATERIALIZED VIEW " + materializedView).getOnlyValue().toString())
                .contains("refresh_schedule = '0 0 * * *'")
                .contains("storage_schema = '" + schemaName + "'");
        workScheduler.runScheduledRefreshesForJobId(workScheduler.getRequiredJobScheduleId(materializedView));
        assertThat(getExplainPlan("SELECT * FROM " + materializedView, ExplainType.Type.IO)).doesNotContain("tpch.tiny.nation");

        assertUpdate("DROP MATERIALIZED VIEW " + materializedView);
    }

    @Test
    public void testExternallyDroppedScheduledRefresh()
    {
        CatalogSchemaTableName materializedView = new CatalogSchemaTableName(
                TEST_CATALOG,
                new SchemaTableName(schemaName, "test_dropped_scheduled_refresh" + randomNameSuffix()));

        computeActual("CREATE MATERIALIZED VIEW " + materializedView + " WITH (refresh_schedule = '0 0 * * *') AS SELECT * FROM tpch.tiny.nation");
        workScheduler.runScheduledRefreshesForJobId(workScheduler.getRequiredJobScheduleId(materializedView));

        workScheduler.deleteJobSchedule(getSession().toConnectorSession(), workScheduler.getRequiredJobScheduleId(materializedView));
        assertThat(computeActual("SHOW CREATE MATERIALIZED VIEW " + materializedView).getOnlyValue().toString())
                .doesNotContain("refresh_schedule")
                .contains("storage_schema = '" + schemaName + "'");
        assertThat(getExplainPlan("SELECT * FROM " + materializedView, ExplainType.Type.IO)).doesNotContain("nation");
        assertUpdate("DROP MATERIALIZED VIEW " + materializedView);
    }

    @Test
    public void testAlterScheduledRefresh()
    {
        CatalogSchemaTableName materializedView = new CatalogSchemaTableName(
                TEST_CATALOG,
                new SchemaTableName(schemaName, "test_alter_scheduled_refresh" + randomNameSuffix()));

        assertUpdate("CREATE MATERIALIZED VIEW " + materializedView + " WITH (refresh_schedule = '0 0 * * *') AS SELECT * FROM tpch.tiny.nation");
        assertUpdate("ALTER MATERIALIZED VIEW " + materializedView + " SET PROPERTIES refresh_schedule = '1 1 * * *'");

        assertThat(getExplainPlan("SELECT * FROM " + materializedView, ExplainType.Type.IO)).contains("nation");

        workScheduler.runScheduledRefreshesForJobId(workScheduler.getRequiredJobScheduleId(materializedView));

        assertThat(getExplainPlan("SELECT * FROM " + materializedView, ExplainType.Type.IO)).doesNotContain("nation");

        assertThat(computeActual("SHOW CREATE MATERIALIZED VIEW " + materializedView).getOnlyValue().toString())
                .contains("refresh_schedule = '1 1 * * *'")
                .contains("storage_schema = '" + schemaName + "'")
                .doesNotContain("refresh_schedule_timezone");

        workScheduler.deleteJobSchedule(getSession().toConnectorSession(), workScheduler.getRequiredJobScheduleId(materializedView));
        assertUpdate("ALTER MATERIALIZED VIEW " + materializedView + " SET PROPERTIES refresh_schedule = '2 2 * * *'");
        assertThat(computeActual("SHOW CREATE MATERIALIZED VIEW " + materializedView).getOnlyValue().toString())
                .contains("refresh_schedule = '2 2 * * *'")
                .contains("storage_schema = '" + schemaName + "'");

        assertUpdate("ALTER MATERIALIZED VIEW " + materializedView + " SET PROPERTIES refresh_schedule = DEFAULT");
        assertThat(computeActual("SHOW CREATE MATERIALIZED VIEW " + materializedView).getOnlyValue().toString())
                .doesNotContain("refresh_schedule")
                .contains("storage_schema = '" + schemaName + "'");
        assertUpdate("DROP MATERIALIZED VIEW " + materializedView);
    }

    @Test
    public void testScheduledRefreshTimezone()
    {
        CatalogSchemaTableName materializedView = new CatalogSchemaTableName(
                TEST_CATALOG,
                new SchemaTableName(schemaName, "test_scheduled_refresh_timezone" + randomNameSuffix()));

        assertUpdate("CREATE MATERIALIZED VIEW " + materializedView + " WITH (refresh_schedule = '0 0 * * *', refresh_schedule_timezone = 'America/Los_Angeles')" +
                " AS SELECT * FROM tpch.tiny.nation");
        String scheduleId = workScheduler.getRequiredJobScheduleId(materializedView);
        assertThat(workScheduler.getJobSchedule(getSession().toConnectorSession(), scheduleId))
                .contains(new RefreshSchedule("0 0 * * *", Optional.of(ZoneId.of("America/Los_Angeles"))));
        assertUpdate("ALTER MATERIALIZED VIEW " + materializedView + " SET PROPERTIES refresh_schedule = '1 1 * * *'");

        assertThat(workScheduler.getJobSchedule(getSession().toConnectorSession(), scheduleId))
                .contains(new RefreshSchedule("1 1 * * *", Optional.of(ZoneId.of("America/Los_Angeles"))));

        assertThat(computeActual("SHOW CREATE MATERIALIZED VIEW " + materializedView).getOnlyValue().toString())
                .contains("refresh_schedule = '1 1 * * *'")
                .contains("refresh_schedule_timezone = 'America/Los_Angeles'");

        assertUpdate("ALTER MATERIALIZED VIEW " + materializedView + " SET PROPERTIES refresh_schedule_timezone = 'America/New_York'");

        assertThat(workScheduler.getJobSchedule(getSession().toConnectorSession(), scheduleId))
                .contains(new RefreshSchedule("1 1 * * *", Optional.of(ZoneId.of("America/New_York"))));

        assertThat(computeActual("SHOW CREATE MATERIALIZED VIEW " + materializedView).getOnlyValue().toString())
                .contains("refresh_schedule = '1 1 * * *'")
                .contains("refresh_schedule_timezone = 'America/New_York'");

        assertUpdate("ALTER MATERIALIZED VIEW " + materializedView + " SET PROPERTIES refresh_schedule_timezone = DEFAULT");

        assertThat(workScheduler.getJobSchedule(getSession().toConnectorSession(), scheduleId))
                .contains(new RefreshSchedule("1 1 * * *", Optional.empty()));
        assertThat(computeActual("SHOW CREATE MATERIALIZED VIEW " + materializedView).getOnlyValue().toString())
                .contains("refresh_schedule = '1 1 * * *'")
                .doesNotContain("refresh_schedule_timezone");

        assertUpdate("ALTER MATERIALIZED VIEW " + materializedView + " SET PROPERTIES refresh_schedule_timezone = 'America/New_York'");
        assertUpdate("ALTER MATERIALIZED VIEW " + materializedView + " SET PROPERTIES refresh_schedule = DEFAULT");

        assertThat(workScheduler.getJobSchedule(getSession().toConnectorSession(), scheduleId)).isEmpty();
        assertThat(computeActual("SHOW CREATE MATERIALIZED VIEW " + materializedView).getOnlyValue().toString())
                .doesNotContain("refresh_schedule")
                .doesNotContain("refresh_schedule_timezone");

        assertUpdate("DROP MATERIALIZED VIEW " + materializedView);
    }

    @Test
    public void testCreateOrReplaceWithNewSchedule()
    {
        CatalogSchemaTableName materializedView = new CatalogSchemaTableName(
                TEST_CATALOG,
                new SchemaTableName(schemaName, "test_create_or_replace_schedule_" + randomNameSuffix()));

        assertUpdate("CREATE OR REPLACE MATERIALIZED VIEW " + materializedView + " " +
                "WITH (refresh_schedule = '0 0 * * *') " +
                "AS SELECT * FROM tpch.tiny.nation");

        String scheduleId = workScheduler.getRequiredJobScheduleId(materializedView);
        Optional<RefreshSchedule> jobSchedule = workScheduler.getJobSchedule(getSession().toConnectorSession(), scheduleId);
        assertThat(jobSchedule.get().cronExpression()).contains("0 0 * * *");
        assertThat(jobSchedule.get().timeZone()).isEmpty();

        assertUpdate("CREATE OR REPLACE MATERIALIZED VIEW " + materializedView + " " +
                "WITH (refresh_schedule = '1 1 * * *') " +
                "AS SELECT * FROM tpch.tiny.nation");

        assertThat(workScheduler.getRequiredJobScheduleId(materializedView)).isEqualTo(scheduleId);
        assertThat(workScheduler.getJobSchedule(getSession().toConnectorSession(), scheduleId).map(RefreshSchedule::cronExpression)).contains("1 1 * * *");

        workScheduler.runScheduledRefreshesForJobId(workScheduler.getRequiredJobScheduleId(materializedView));

        assertUpdate("CREATE OR REPLACE MATERIALIZED VIEW " + materializedView + " " +
                "WITH (refresh_schedule = '2 2 * * *') " +
                "AS SELECT * FROM tpch.tiny.nation");

        assertThat(workScheduler.getRequiredJobScheduleId(materializedView)).isEqualTo(scheduleId);
        assertThat(workScheduler.getJobSchedule(getSession().toConnectorSession(), scheduleId).map(RefreshSchedule::cronExpression)).contains("2 2 * * *");
        assertUpdate("DROP MATERIALIZED VIEW " + materializedView);
    }

    @Test
    public void testSetScheduleOnExistingMaterializedView()
    {
        CatalogSchemaTableName materializedView = new CatalogSchemaTableName(
                TEST_CATALOG,
                new SchemaTableName(schemaName, "test_set_schedule_on_existing_mv_" + randomNameSuffix()));

        assertUpdate("CREATE MATERIALIZED VIEW " + materializedView + " AS SELECT * FROM tpch.tiny.nation");
        assertUpdate("ALTER MATERIALIZED VIEW " + materializedView + " SET PROPERTIES refresh_schedule = '1 1 * * *'");
        assertThat(computeActual("SHOW CREATE MATERIALIZED VIEW " + materializedView).getOnlyValue().toString())
                .contains("refresh_schedule = '1 1 * * *'")
                .contains("storage_schema = '" + schemaName + "'");

        assertUpdate("DROP MATERIALIZED VIEW " + materializedView);
    }

    @Test
    public void testDroppingMaterializedViewDeletesScheduledRefresh()
    {
        CatalogSchemaTableName materializedView = new CatalogSchemaTableName(
                TEST_CATALOG,
                new SchemaTableName(schemaName, "test_dropping_mv_deletes_scheduled_refresh" + randomNameSuffix()));

        assertUpdate("CREATE MATERIALIZED VIEW " + materializedView + " WITH (refresh_schedule = '0 0 * * *') AS SELECT * FROM tpch.tiny.nation");

        assertUpdate("DROP MATERIALIZED VIEW " + materializedView);
        assertThat(workScheduler.getJobScheduleId(materializedView)).isEmpty();
    }

    @Test
    public void testShowCreateMaterializedViewWithCatalogWithoutScheduling()
    {
        CatalogSchemaTableName materializedView = new CatalogSchemaTableName(
                TEST_CATALOG,
                schemaName,
                "test_show_mv_without_scheduling" + randomNameSuffix());
        CatalogSchemaTableName materializedViewWithoutScheduling = new CatalogSchemaTableName(
                TEST_CATALOG_WITHOUT_SCHEDULING,
                schemaName,
                materializedView.getSchemaTableName().getTableName());

        assertUpdate("CREATE MATERIALIZED VIEW " + materializedView + " WITH (refresh_schedule = '0 0 * * *') AS SELECT * FROM tpch.tiny.nation");

        assertThat(computeActual("SHOW CREATE MATERIALIZED VIEW " + materializedView).getOnlyValue().toString())
                .contains("refresh_schedule");

        assertThat(computeActual("SHOW CREATE MATERIALIZED VIEW " + materializedViewWithoutScheduling).getOnlyValue().toString())
                .doesNotContain("refresh_schedule");

        assertUpdate("DROP MATERIALIZED VIEW " + materializedView);
    }

    @Test
    public void testMaterializedViewWithInvalidRefreshInterval()
    {
        CatalogSchemaTableName materializedView = new CatalogSchemaTableName(
                TEST_CATALOG,
                new SchemaTableName(schemaName, "test_invalid_refresh_interval" + randomNameSuffix()));

        assertThat(query(getSession(), "CREATE MATERIALIZED VIEW " + materializedView + " WITH (refresh_schedule = 'not cron') AS SELECT 1 AS c"))
                .failure().hasStackTraceContaining("Refresh interval is not cron string");

        assertUpdate("CREATE MATERIALIZED VIEW " + materializedView + " AS SELECT * FROM tpch.tiny.nation");
        assertThat(query(getSession(), "ALTER MATERIALIZED VIEW " + materializedView + " SET PROPERTIES refresh_schedule = 'not cron'"))
                .failure().hasStackTraceContaining("Refresh interval is not cron string");
    }
}
