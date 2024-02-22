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

package io.trino.server.resultscache;

import io.airlift.log.Logger;
import io.trino.execution.QueryPreparer.PreparedQuery;
import io.trino.metadata.TableHandle;
import io.trino.spi.QueryId;
import io.trino.spi.connector.CatalogHandle.CatalogHandleType;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.tree.Query;

import java.util.Optional;

import static io.trino.server.resultscache.ResultsCacheEntry.ResultsCacheResult.Status.EXECUTE_STATEMENT;
import static io.trino.server.resultscache.ResultsCacheEntry.ResultsCacheResult.Status.NOT_SELECT;
import static io.trino.server.resultscache.ResultsCacheEntry.ResultsCacheResult.Status.QUERY_HAS_SYSTEM_TABLE;

public class ResultsCacheAnalyzer
{
    private static final Logger log = Logger.get(ResultsCacheAnalyzer.class);

    public Optional<FilteredResultsCacheEntry> isStatementCacheable(QueryId queryId, PreparedQuery preparedQuery, Analysis analysis)
    {
        if (preparedQuery.isExecuteStatement()) {
            log.debug("QueryId: %s, statement is EXECUTE statement, not caching", queryId);
            return Optional.of(new FilteredResultsCacheEntry(EXECUTE_STATEMENT));
        }

        if (!(preparedQuery.getStatement() instanceof Query)) {
            log.debug("QueryId: %s, statement is not a Query, not caching", queryId);
            return Optional.of(new FilteredResultsCacheEntry(NOT_SELECT));
        }

        for (TableHandle tableHandle : analysis.getTables()) {
            CatalogHandleType type = tableHandle.getCatalogHandle().getType();
            if (type.isInternal()) {
                log.debug("QueryId: %s, query uses internal table %s, not caching", queryId, tableHandle);
                return Optional.of(new FilteredResultsCacheEntry(QUERY_HAS_SYSTEM_TABLE));
            }
        }

        log.debug("QueryId: %s, statement is cacheable", queryId);
        return Optional.empty();
    }
}
