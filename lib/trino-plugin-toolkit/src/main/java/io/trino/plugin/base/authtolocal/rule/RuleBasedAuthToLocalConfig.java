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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import java.util.Optional;

public class RuleBasedAuthToLocalConfig
{
    private String configFile;
    private Duration refreshPeriod;

    public Optional<@FileExists String> getConfigFile()
    {
        return Optional.ofNullable(configFile);
    }

    @Config("auth-to-local.config-file")
    @ConfigDescription("Path to user translation file")
    public RuleBasedAuthToLocalConfig setConfigFile(String authToLocalConfigFile)
    {
        this.configFile = authToLocalConfigFile;
        return this;
    }

    @MinDuration("1ms")
    public Duration getRefreshPeriod()
    {
        return refreshPeriod;
    }

    @Config("auth-to-local.refresh-period")
    @ConfigDescription("Refresh interval for loading the translation file after last update")
    public RuleBasedAuthToLocalConfig setRefreshPeriod(Duration refreshPeriod)
    {
        this.refreshPeriod = refreshPeriod;
        return this;
    }
}
