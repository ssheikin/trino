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

import com.google.common.collect.ImmutableList;
import io.trino.client.StatementStats;
import io.trino.spi.WorkScheduler;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class TestingWorkScheduler
        implements WorkScheduler
{
    private final Map<String, MaterializedViewRefresh> jobs = new ConcurrentHashMap<>();
    private final Map<CatalogSchemaTableName, List<MaterializedViewRefreshRecord>> refreshHistory = new ConcurrentHashMap<>();
    private final Supplier<QueryRunner> queryRunner;

    public TestingWorkScheduler(Supplier<QueryRunner> queryRunner)
    {
        this.queryRunner = requireNonNull(queryRunner, "queryRunner is null");
    }

    @Override
    public String createMaterializedViewRefreshJob(ConnectorSession session, String catalogName, String schemaName, String materializedViewName, RefreshSchedule schedule)
    {
        String id = UUID.randomUUID().toString();
        jobs.put(id, new MaterializedViewRefresh(new CatalogSchemaTableName(catalogName, schemaName, materializedViewName), schedule));
        return id;
    }

    @Override
    public Optional<RefreshSchedule> getJobSchedule(ConnectorSession session, String jobId)
    {
        return Optional.ofNullable(jobs.get(jobId)).map(MaterializedViewRefresh::schedule);
    }

    @Override
    public void deleteJobSchedule(ConnectorSession session, String jobId)
    {
        jobs.remove(jobId);
    }

    @Override
    public boolean updateJobSchedule(ConnectorSession session, String jobId, RefreshSchedule schedule)
    {
        return jobs.computeIfPresent(jobId, (_, existing) -> existing.withSchedule(schedule)) != null;
    }

    @Override
    public boolean updateMaterializedViewName(ConnectorSession session, String jobId, String materializedViewName)
    {
        return jobs.computeIfPresent(jobId, (_, existing) -> existing.withMaterializedViewName(materializedViewName)) != null;
    }

    @Override
    public List<MaterializedViewRefreshRecord> listRefreshHistory(String catalogName, Collection<SchemaTableName> materializedViews)
    {
        ImmutableList.Builder<MaterializedViewRefreshRecord> results = ImmutableList.builder();
        for (SchemaTableName materializedView : materializedViews) {
            results.addAll(refreshHistory.getOrDefault(new CatalogSchemaTableName(catalogName, materializedView.getSchemaName(), materializedView.getTableName()), List.of())
                    .stream()
                    .filter(record -> record.materializedView().equals(materializedView))
                    .collect(toImmutableList())); // should always be true, but just in case
        }
        return results.build();
    }

    public void runScheduledRefreshesForJobId(String jobId)
    {
        Instant startedAt = Instant.now();
        MaterializedResult result = refreshMaterializedView(jobs.get(jobId));
        List<MaterializedViewRefreshRecord> refreshRecords = refreshHistory.get(jobs.get(jobId).table());
        if (refreshRecords == null) {
            refreshHistory.put(jobs.get(jobId).table(), List.of(getMaterializedViewRefreshRecord(jobId, result, startedAt)));
        }
        else {
            refreshRecords.add(getMaterializedViewRefreshRecord(jobId, result, startedAt));
        }
    }

    private MaterializedViewRefreshRecord getMaterializedViewRefreshRecord(String jobId, MaterializedResult result, Instant startedAt)
    {
        return new MaterializedViewRefreshRecord(
                jobs.get(jobId).table().getSchemaTableName(),
                result.getStatementStats().map(StatementStats::getState).orElse("COMPLETED"), // For testing purposes, we assume all jobs are completed without errors
                Optional.of(result.getSession().getQueryId()),
                startedAt,
                startedAt,
                Optional.of(startedAt),
                startedAt.plusNanos(result.getStatementStats().map(StatementStats::getWallTimeMillis).orElse(0L)),
                Optional.empty());
    }

    public String getRequiredJobScheduleId(CatalogSchemaTableName materializedView)
    {
        return getJobScheduleId(materializedView).orElseThrow();
    }

    public Optional<String> getJobScheduleId(CatalogSchemaTableName materializedView)
    {
        return jobs.entrySet().stream()
                .filter(entry -> entry.getValue().table().equals(materializedView))
                .map(Map.Entry::getKey)
                .findAny();
    }

    private MaterializedResult refreshMaterializedView(MaterializedViewRefresh job)
    {
        return queryRunner.get().execute("REFRESH MATERIALIZED VIEW " + job.table());
    }

    private record MaterializedViewRefresh(CatalogSchemaTableName table, RefreshSchedule schedule)
    {
        private MaterializedViewRefresh
        {
            requireNonNull(table, "table is null");
            requireNonNull(schedule, "schedule is null");
        }

        public MaterializedViewRefresh withSchedule(RefreshSchedule schedule)
        {
            return new MaterializedViewRefresh(table, schedule);
        }

        public MaterializedViewRefresh withMaterializedViewName(String materializedViewName)
        {
            return new MaterializedViewRefresh(new CatalogSchemaTableName(table.getCatalogName(), table.getSchemaTableName().getSchemaName(), materializedViewName), schedule);
        }
    }
}
