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
package io.trino.plugin.hive.azure;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.testing.QueryRunner;

import static io.trino.plugin.hive.azure.AzureAdSupport.AZURE_AD_IDP_NAME;

public class TestHiveAbfsPassthroughMultiIdp
        extends BaseTestHiveAbfsPassthrough
{
    @Override
    protected final QueryRunner createQueryRunner()
            throws Exception
    {
        initHiveHadoop();
        return HiveQueryRunner.builder(AzureAdSupport.createDefaultUserSessionWithIdp())
                .setHiveProperties(ImmutableMap.<String, String>builder()
                        .put("hive.metastore", "thrift")
                        .put("hive.metastore.uri", hiveHadoop.getHiveMetastoreEndpoint().toString())
                        .put("hive.azure.abfs.oauth2.passthrough", "true")
                        .put("hive.idp-name", AZURE_AD_IDP_NAME.toString())
                        .put("fs.hadoop.enabled", "true")
                        .put("hive.security", "allow-all")
                        .buildOrThrow())
                .build();
    }

    @Override
    protected Session nonAuthorizedUserSession()
            throws Exception
    {
        return AzureAdSupport.createAzureUserSession(
                CLIENT_ID,
                clientSecret,
                SCOPE,
                AZURE_AD_IDP_NAME);
    }
}
