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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

public class AzureAdConfig
{
    private boolean oauthPassthrough;

    public boolean isOauthPassthrough()
    {
        return oauthPassthrough;
    }

    @Config("hive.azure.abfs.oauth2.passthrough")
    @ConfigDescription("If true, reuse the same Azure AD token which was used for authentication with SEP for reading from Azure Blob Storage. Token should grant \"user_impersonation\" API permission for the Azure Storage")
    public AzureAdConfig setOauthPassthrough(boolean oauthPassthrough)
    {
        this.oauthPassthrough = oauthPassthrough;
        return this;
    }
}
