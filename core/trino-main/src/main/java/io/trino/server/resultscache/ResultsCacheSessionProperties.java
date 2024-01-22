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
import io.trino.Session;
import io.trino.SystemSessionPropertiesProvider;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.longProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;

public final class ResultsCacheSessionProperties
        implements SystemSessionPropertiesProvider
{
    public static final String CACHE_KEY = "results_cache_key";
    public static final String CACHE_ENTRY_MAX_SIZE_BYTES = "results_cache_entry_max_size_bytes";

    // These properties are consumed by the dispatcher and not used within Trino
    public static final String SKIP_RESULTS_CACHE_SESSION_PROPERTY = "skip_results_cache";

    public static final List<PropertyMetadata<?>> SESSION_PROPERTIES = ImmutableList.of(
            stringProperty(
                    CACHE_KEY,
                    "Unique key to identify the cache entry",
                    null,
                    false),
            longProperty(
                    CACHE_ENTRY_MAX_SIZE_BYTES,
                    "Maximum size for a cache entry in bytes",
                    null,
                    false),
            booleanProperty(
                    SKIP_RESULTS_CACHE_SESSION_PROPERTY,
                    "Skip using the results cache regardless of if cached results are available",
                    null,
                    false));

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return SESSION_PROPERTIES;
    }

    public static Optional<String> getResultsCacheKey(Session session)
    {
        return Optional.ofNullable(session.getSystemProperty(CACHE_KEY, String.class));
    }

    public static Optional<Long> getResultsCacheEntryMaxSizeBytes(Session session)
    {
        return Optional.ofNullable(session.getSystemProperty(CACHE_ENTRY_MAX_SIZE_BYTES, Long.class));
    }
}
