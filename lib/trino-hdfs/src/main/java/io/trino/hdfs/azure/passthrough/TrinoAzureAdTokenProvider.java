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
package io.trino.hdfs.azure.passthrough;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee;

import java.util.Date;

import static io.trino.hdfs.azure.passthrough.TrinoAzureAdConfigurationUpdater.TRINO_INTERNAL_ACCESS_TOKEN;

// https://hadoop.apache.org/docs/stable/hadoop-azure/abfs.html#Custom_OAuth_2.0_Token_Provider
public class TrinoAzureAdTokenProvider
        implements CustomTokenProviderAdaptee
{
    private String configuredToken;

    @Override
    public void initialize(Configuration configuration, String accountName)
    {
        configuredToken = configuration.get(TRINO_INTERNAL_ACCESS_TOKEN);
    }

    @Override
    public String getAccessToken()
    {
        return configuredToken;
    }

    @Override
    public Date getExpiryTime()
    {
        // we don't need to cache the result of getAccessToken method, see JavaDoc for CustomTokenProviderAdaptee#getExpiryTime
        return new Date();
    }
}
