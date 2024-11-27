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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestCachingAuthToLocalConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(CachingAuthToLocalConfig.class)
                .setCacheTtl(new Duration(0, SECONDS))
                .setCacheMaximumSize(10_000));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("auth-to-local.cache-ttl", "1d")
                .put("auth-to-local.cache-maximum-size", "10")
                .buildOrThrow();

        CachingAuthToLocalConfig expected = new CachingAuthToLocalConfig()
                .setCacheTtl(new Duration(1, DAYS))
                .setCacheMaximumSize(10);

        assertFullMapping(properties, expected);
    }
}
