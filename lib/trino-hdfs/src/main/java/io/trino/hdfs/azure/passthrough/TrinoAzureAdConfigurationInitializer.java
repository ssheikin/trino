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

import com.google.inject.Inject;
import io.trino.hdfs.ConfigurationInitializer;
import io.trino.hdfs.azure.HiveAzureConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.services.AuthType;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME;

public class TrinoAzureAdConfigurationInitializer
        implements ConfigurationInitializer
{
    @Inject
    public TrinoAzureAdConfigurationInitializer(HiveAzureConfig hiveAzureConfig)
    {
        // TrinoAzureConfigurationInitializer validates more
        checkState(
                hiveAzureConfig.getAbfsAccessKey().orElse("").isEmpty() && hiveAzureConfig.getAbfsOAuthClientSecret().orElse("").isEmpty(),
                "When using Azure AD pass-through, no other ABFS credentials should be set");
    }

    @Override
    public void initializeConfiguration(Configuration config)
    {
        config.set(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, AuthType.Custom.name());
        config.set(FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME, TrinoAzureAdTokenProvider.class.getName());
    }
}
