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

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.spi.QueryId;

import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.trino.server.resultscache.ResultsCacheSessionProperties.getResultsCacheKey;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class ResultsCacheManager
{
    private static final Logger log = Logger.get(ResultsCacheManager.class);
    private final long configuredMaxSize;
    private final ResultsCacheClient resultsCacheClient;
    private final ListeningExecutorService executorService;

    @Inject
    public ResultsCacheManager(CacheClient cacheClient, ResultsCacheConfig config)
    {
        this.configuredMaxSize = config.getMaxResultsSize().toBytes();
        this.resultsCacheClient = new ResultsCacheClient(
                config.getCacheEndpoint(),
                requireNonNull(cacheClient, "cacheClient is null"));
        this.executorService = listeningDecorator(newFixedThreadPool(config.getCacheUploadThreads(), threadsNamed("resultscache-upload-%s")));
    }

    public ActiveResultsCacheEntry createResultsCacheEntry(
            ResultsCacheState resultsCacheParameters,
            QueryId queryId,
            String query,
            Optional<String> sessionCatalog,
            Optional<String> sessionSchema,
            Optional<String> queryType,
            Optional<String> updateType)
    {
        Optional<Long> requestMaxSizeOptional = resultsCacheParameters.maximumSizeBytes();
        long maximumSizeBytes = requestMaxSizeOptional.map(requestMaxSize -> {
            if (requestMaxSize > configuredMaxSize) {
                log.debug("Maximum results size configured in request %s is larger than globally configured maximum %s.", requestMaxSize, configuredMaxSize);
                return configuredMaxSize;
            }
            return requestMaxSize;
        }).orElse(configuredMaxSize);

        log.debug("QueryId: %s, created ResultsCacheEntry with key %s", queryId, resultsCacheParameters.key());
        return new ActiveResultsCacheEntry(
                resultsCacheParameters.key(),
                queryId,
                query,
                sessionCatalog,
                sessionSchema,
                queryType,
                updateType,
                maximumSizeBytes,
                resultsCacheClient,
                executorService);
    }

    public static Optional<ResultsCacheState> createResultsCacheParameters(Session session)
    {
        return getResultsCacheKey(session).map(cacheKey -> {
            log.debug("QueryId: %s, statement had cache key %s", session.getQueryId(), cacheKey);
            return new ResultsCacheState(
                    cacheKey,
                    ResultsCacheSessionProperties.getResultsCacheEntryMaxSizeBytes(session)); });
    }
}
