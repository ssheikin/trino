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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.concurrent.MoreFutures;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.client.Column;
import io.trino.client.QueryData;
import io.trino.execution.Input;
import io.trino.server.protocol.JsonBytesQueryData;
import io.trino.server.protocol.QueryResultRows;
import io.trino.server.resultscache.CacheEntry.Reference;
import io.trino.spi.QueryId;
import io.trino.spi.eventlistener.TableInfo;
import io.trino.sql.analyzer.Output;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.server.resultscache.ResultsCacheEntry.ResultsCacheResult.Status.CACHED;
import static io.trino.server.resultscache.ResultsCacheEntry.ResultsCacheResult.Status.CACHING;
import static io.trino.server.resultscache.ResultsCacheEntry.ResultsCacheResult.Status.NO_COLUMNS;
import static io.trino.server.resultscache.ResultsCacheEntry.ResultsCacheResult.Status.OVER_MAX_SIZE;
import static io.trino.server.resultscache.ResultsCacheEntry.ResultsCacheResult.Status.PROTOCOL_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.function.Predicate.not;

public class ActiveResultsCacheEntry
        implements ResultsCacheEntry
{
    private static final Logger log = Logger.get(ActiveResultsCacheEntry.class);
    private final String cacheKey;
    private final long cacheEpoch;
    private final QueryId queryId;
    private final String query;
    private final String user;
    private final long maximumSize;
    private final CacheClient cacheClient;
    private final ListeningExecutorService executorService;
    @GuardedBy("this")
    private final List<DoneCallback> doneCallbacks = new ArrayList<>();
    @GuardedBy("this")
    private long currentSize;
    @GuardedBy("this")
    private boolean doneCalled;
    @GuardedBy("this")
    private boolean valid = true;
    @GuardedBy("this")
    private Optional<ResultsData> resultsData = Optional.empty();
    @GuardedBy("this")
    private ResultsCacheResult entryResult;

    public ActiveResultsCacheEntry(
            String cacheKey,
            long cacheEpoch,
            QueryId queryId,
            String query,
            String user,
            long maximumSize,
            CacheClient cacheClient,
            ListeningExecutorService executorService)
    {
        this.cacheKey = requireNonNull(cacheKey, "key is null");
        this.cacheEpoch = cacheEpoch;
        this.entryResult = new ResultsCacheResult(CACHING);
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.query = requireNonNull(query, "query is null");
        this.user = requireNonNull(user, "user is null");
        checkArgument(maximumSize > 0, "maximumSize is <= 0");
        this.maximumSize = maximumSize;
        this.cacheClient = requireNonNull(cacheClient, "cacheClient is null");
        this.executorService = requireNonNull(executorService, "executorService is null");
    }

    @Override
    public void done()
    {
        List<CompletionCallback> completionActions = new ArrayList<>();
        synchronized (this) {
            if (!doneCalled) {
                if (valid) {
                    if (resultsData.isPresent()) {
                        log.debug("QueryId: %s, done called and cache entry is valid, uploading to cache", queryId);
                        entryResult = new ResultsCacheResult(CACHED, currentSize);
                        submitAsyncUpload(OffsetDateTime.now().toInstant(), resultsData.orElseThrow());
                        resultsData = Optional.empty();
                    }
                    else {
                        log.debug("QueryId: %s, done called and cache entry is empty.  Not uploading", queryId);
                    }
                    completionActions.add(() -> doneCallbacks.forEach(DoneCallback::done));
                }
                doneCalled = true;
            }
        }
        completionActions.forEach(CompletionCallback::apply);
    }

    @Override
    public boolean isDone()
    {
        return doneCalled || !valid;
    }

    @Override
    public boolean addTransitionToDoneCallback(DoneCallback doneCallback)
    {
        synchronized (this) {
            if (isDone()) {
                return false;
            }
            doneCallbacks.add(doneCallback);
        }
        return true;
    }

    @Override
    public Optional<ResultsCacheResult> getEntryResult()
    {
        return Optional.of(entryResult);
    }

    public void appendResults(
            Session session,
            Set<Input> inputs,
            Optional<Output> output,
            List<TableInfo> referencedTables,
            List<Column> columns,
            QueryResultRows resultRows,
            QueryData queryData)
    {
        if (queryData == null || queryData.isNull()) {
            return;
        }
        List<CompletionCallback> completionCallbacks = new ArrayList<>();
        try {
            synchronized (this) {
                if (doneCalled && !resultRows.isEmpty()) {
                    throw new IllegalStateException("result rows added after done method called");
                }

                if (isDone()) {
                    return;
                }

                if (resultsData.isEmpty()) {
                    if (columns == null && resultRows.isEmpty()) {
                        log.debug("QueryId: %s, received null columns and empty rows for cache entry %s, ignoring", queryId, cacheKey);
                        return;
                    }
                    else if (columns == null) {
                        log.debug("QueryId: %s, null columns provided for results cache entry %s, not caching", queryId, cacheKey);
                        completionCallbacks.add(setInvalidState(NO_COLUMNS));
                        return;
                    }
                    ImmutableSet.Builder<Reference> tableReferencesBuilder = ImmutableSet.builder();
                    inputs.forEach(entry -> tableReferencesBuilder.add(new Reference(entry.getCatalogName(), entry.getSchema(), entry.getTable(), false)));
                    output.ifPresent(entry -> tableReferencesBuilder.add(new Reference(entry.getCatalogName(), entry.getSchema(), entry.getTable(), true)));
                    Set<Reference> tablesReferences = tableReferencesBuilder.build();
                    Set<Reference> viewsReferences = referencedTables.stream()
                            .map(entry -> new Reference(entry.getCatalog(), entry.getSchema(), entry.getTable(), false))
                            .filter(not(tablesReferences::contains))
                            .collect(toImmutableSet());
                    switch (queryData) {
                        case JsonBytesQueryData _ -> resultsData = Optional.of(new ResultsData.DirectResultsData(columns, tablesReferences, viewsReferences));
                        default -> {
                            completionCallbacks.add(setInvalidState(PROTOCOL_ERROR));
                            return;
                        }
                    }
                }

                long retainedSizeInBytes = resultRows.countRetainedSizeInBytes();
                currentSize += retainedSizeInBytes;
                if (retainedSizeInBytes > 0) {
                    entryResult = entryResult.withCurrentSize(currentSize);
                }

                if (currentSize > maximumSize) {
                    log.debug("QueryId: %s, results exceeded maximum size of %s bytes, not caching", queryId, maximumSize);
                    completionCallbacks.add(setInvalidState(OVER_MAX_SIZE));
                    return;
                }

                Optional<ResultsCacheResult.Status> status = resultsData.get().append(queryData);
                if (status.isPresent()) {
                    log.debug("QueryId: %s, error while appending to cache entry, not caching results", queryId);
                    completionCallbacks.add(setInvalidState(status.get()));
                }
                else {
                    log.debug("QueryId: %s, appending to cache entry, %s bytes, %s current total size", queryId, retainedSizeInBytes, currentSize);
                }
            }
        }
        finally {
            completionCallbacks.forEach(CompletionCallback::apply);
        }
    }

    @GuardedBy("this")
    private CompletionCallback setInvalidState(ResultsCacheResult.Status invalidState)
    {
        checkState(valid, "invalid state already set");

        valid = false;
        entryResult = new ResultsCacheResult(invalidState);
        return () -> doneCallbacks.forEach(DoneCallback::done);
    }

    @GuardedBy("this")
    private void submitAsyncUpload(Instant createdTime, ResultsData resultsData)
    {
        checkState(valid, "attempting to upload results in invalid state");

        ListenableFuture<?> submitFuture = executorService.submit(() ->
                cacheClient.insertCacheEntry(
                        new CacheEntry(
                                cacheKey,
                                cacheEpoch,
                                createdTime,
                                user,
                                queryId.toString(),
                                query,
                                resultsData.columns(),
                                resultsData.data(),
                                Optional.of(resultsData.getTablesReferences()),
                                Optional.of(resultsData.getViewsReferences()))));
        MoreFutures.addExceptionCallback(submitFuture, throwable ->
                log.error(throwable, "Upload to cache failed"));
    }

    private sealed interface ResultsData
    {
        QueryData data();

        List<Column> columns();

        Optional<ResultsCacheResult.Status> append(QueryData queryData);

        Set<Reference> getTablesReferences();

        Set<Reference> getViewsReferences();

        final class DirectResultsData
                implements ResultsData
        {
            private final Set<Reference> tablesReferences;
            private final Set<Reference> viewReferences;
            private final List<Column> columns;
            private JsonBytesQueryData data;

            public DirectResultsData(List<Column> columns, Set<Reference> tablesReferences, Set<Reference> viewsReferences)
            {
                this.tablesReferences = ImmutableSet.copyOf(tablesReferences);
                this.viewReferences = ImmutableSet.copyOf(viewsReferences);
                this.columns = ImmutableList.copyOf(columns);
            }

            @Override
            public List<Column> columns()
            {
                return columns;
            }

            @Override
            public JsonBytesQueryData data()
            {
                return data;
            }

            @Override
            public Set<Reference> getTablesReferences()
            {
                return tablesReferences;
            }

            @Override
            public Set<Reference> getViewsReferences()
            {
                return viewReferences;
            }

            @Override
            public Optional<ResultsCacheResult.Status> append(QueryData queryData)
            {
                if (!(queryData instanceof JsonBytesQueryData jsonBytesQueryData)) {
                    return Optional.of(PROTOCOL_ERROR);
                }
                try {
                    append(jsonBytesQueryData);
                    return Optional.empty();
                }
                catch (Exception e) {
                    return Optional.of(PROTOCOL_ERROR);
                }
            }

            private void append(JsonBytesQueryData current)
            {
                if (data == null) {
                    data = current;
                }
                else {
                    data = data.mergeWith(current);
                }
            }
        }
    }

    private interface CompletionCallback
    {
        void apply();
    }
}
