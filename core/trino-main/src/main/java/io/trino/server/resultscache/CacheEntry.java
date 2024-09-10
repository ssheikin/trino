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
import io.trino.client.Column;
import io.trino.spi.Page;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public record CacheEntry(
        String key,
        long epoch,
        Instant created,
        String user,
        String queryId,
        String queryText,
        List<Column> columns,
        List<Page> rows,
        Optional<Set<Reference>> tables,
        Optional<Set<Reference>> views)
{
    public CacheEntry
    {
        requireNonNull(key, "key is null");
        requireNonNull(created, "created is null");
        requireNonNull(user, "user is null");
        requireNonNull(queryId, "queryId is null");
        requireNonNull(queryText, "queryText is null");
        columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        // Nested row lists can have null values, ImmutableList does not support null values
        rows = ImmutableList.copyOf(requireNonNull(rows, "rows is null"));
        tables = tables.map(ImmutableSet::copyOf);
        views = views.map(ImmutableSet::copyOf);
    }

    public record Reference(String catalog, String schema, String name, boolean output)
    {
        public Reference
        {
            requireNonNull(catalog, "catalog is null");
            requireNonNull(schema, "schema is null");
            requireNonNull(name, "name is null");
        }
    }
}
