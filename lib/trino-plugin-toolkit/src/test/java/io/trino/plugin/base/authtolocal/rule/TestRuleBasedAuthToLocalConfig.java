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
package io.trino.plugin.base.authtolocal.rule;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestRuleBasedAuthToLocalConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(RuleBasedAuthToLocalConfig.class)
                .setConfigFile(null)
                .setRefreshPeriod(null));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        File configFile = Files.createTempFile(null, null).toFile();
        configFile.deleteOnExit();

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("auth-to-local.config-file", configFile.getPath())
                .put("auth-to-local.refresh-period", "1s")
                .buildOrThrow();

        RuleBasedAuthToLocalConfig expected = new RuleBasedAuthToLocalConfig()
                .setConfigFile(configFile.getPath())
                .setRefreshPeriod(Duration.valueOf("1s"));

        assertFullMapping(properties, expected);
    }
}
