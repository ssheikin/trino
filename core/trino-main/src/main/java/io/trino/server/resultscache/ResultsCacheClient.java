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

import com.fasterxml.jackson.core.JsonProcessingException;
import io.airlift.log.Logger;
import io.trino.client.Column;
import io.trino.spi.QueryId;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ResultsCacheClient
{
    private static final Logger log = Logger.get(ResultsCacheClient.class);
    private final String cacheBaseUri;
    private final CacheClient cacheClient;

    public ResultsCacheClient(String cacheBaseUri, CacheClient cacheClient)
    {
        this.cacheBaseUri = requireNonNull(cacheBaseUri, "cacheBaseUri is null");
        this.cacheClient = requireNonNull(cacheClient, "cacheClient is null");
    }

    public void uploadResultsCacheEntry(
            String key,
            QueryId queryId,
            String query,
            Optional<String> sessionCatalog,
            Optional<String> sessionSchema,
            Optional<String> queryType,
            Optional<String> updateType,
            List<Column> columns,
            List<List<Object>> data,
            Instant creation)
    {
        log.debug("Sending cache entry %s for query %s to results cache", key, queryId);
        try {
            cacheClient.insertCacheEntry(
                    cacheBaseUri,
                    createCacheEntry(key, queryId, query, sessionCatalog, sessionSchema, queryType, updateType, columns, data, creation));
        }
        catch (JsonProcessingException ex) {
            throw new RuntimeException("Error serializing results to JSON", ex);
        }
    }

    private static CacheEntry createCacheEntry(
            String cacheKey,
            QueryId queryId,
            String query,
            Optional<String> sessionCatalog,
            Optional<String> sessionSchema,
            Optional<String> queryType,
            Optional<String> updateType,
            List<Column> columns,
            List<List<Object>> data,
            Instant creation)
            throws JsonProcessingException
    {
        return new CacheEntry(
                cacheKey,
                queryId.toString(),
                query,
                sessionCatalog,
                sessionSchema,
                queryType,
                updateType,
                sessionCatalog.stream().toList(),
                columns,
                data,
                creation);
    }
}
