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
import io.trino.client.Column;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Changes to this model require changes to trino-plane-infra smoke tests
 */
public record CacheEntry(
        String key,
        String queryId,
        String queryText,
        Optional<String> sessionCatalog,
        Optional<String> sessionSchema,
        Optional<String> queryType,
        Optional<String> updateType,
        List<String> catalogs,
        List<Column> columns,
        List<List<Object>> rows,
        Instant creation)
{
    public CacheEntry
    {
        requireNonNull(key, "key is null");
        requireNonNull(queryId, "queryId is null");
        requireNonNull(queryText, "queryText is null");
        requireNonNull(sessionCatalog, "sessionCatalog is null");
        requireNonNull(sessionSchema, "sessionSchema is null");
        requireNonNull(queryType, "queryType is null");
        requireNonNull(updateType, "updateType is null");
        catalogs = ImmutableList.copyOf(requireNonNull(catalogs, "catalogs is null"));
        columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        // Nested row lists can have null values, ImmutableList does not support null values
        rows = ImmutableList.copyOf(requireNonNull(rows, "rows is null"));
        requireNonNull(creation, "creation is null");
    }
}
