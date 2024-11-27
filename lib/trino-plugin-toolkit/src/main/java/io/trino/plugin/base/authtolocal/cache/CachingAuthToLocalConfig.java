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
package io.trino.plugin.base.authtolocal.cache;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;

import static java.util.concurrent.TimeUnit.SECONDS;

public class CachingAuthToLocalConfig
{
    private Duration cacheTtl = new Duration(0, SECONDS);
    private long cacheMaximumSize = 10000;

    @Config("auth-to-local.cache-ttl")
    @ConfigDescription("Determines how long user mapping information will be cached")
    public CachingAuthToLocalConfig setCacheTtl(Duration cacheTtl)
    {
        this.cacheTtl = cacheTtl;
        return this;
    }

    public Duration getCacheTtl()
    {
        return cacheTtl;
    }

    @Config("auth-to-local.cache-maximum-size")
    @ConfigDescription("Maximum number of objects stored in the cache")
    public CachingAuthToLocalConfig setCacheMaximumSize(long cacheMaximumSize)
    {
        this.cacheMaximumSize = cacheMaximumSize;
        return this;
    }

    public long getCacheMaximumSize()
    {
        return cacheMaximumSize;
    }
}
