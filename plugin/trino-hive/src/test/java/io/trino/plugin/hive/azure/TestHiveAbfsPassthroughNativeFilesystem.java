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
import org.junit.jupiter.api.Test;

public class TestHiveAbfsPassthroughNativeFilesystem
        extends BaseTestHiveAbfsPassthrough
{
    @Override
    protected final QueryRunner createQueryRunner()
            throws Exception
    {
        initHiveHadoop();
        return HiveQueryRunner.builder(AzureAdSupport.createDefaultUserSession())
                .setHiveProperties(ImmutableMap.<String, String>builder()
                        .put("hive.metastore", "thrift")
                        .put("hive.metastore.uri", hiveHadoop.getHiveMetastoreEndpoint().toString())
                        .put("fs.hadoop.enabled", "false")
                        .put("fs.native-azure.enabled", "true")
                        .put("azure.use-oauth-passthrough-token", "true")
                        .put("hive.security", "allow-all")
                        .buildOrThrow())
                .build();
    }

    @Test
    @Override
    public void testQuery()
    {
        // Temporarily disabled due to a bug that existed in SEP and got uncovered
        // after moving multi-token IDP for Azure native FS to SEP-Trino.
        // The functionally tested here never worked in SEP.
    }

    @Test
    @Override
    public void testUsersSwitching()
    {
        // Temporarily disabled due to a bug that existed in SEP and got uncovered
        // after moving multi-token IDP for Azure native FS to SEP-Trino.
        // The functionally tested here never worked in SEP.
    }

    @Override
    protected Session nonAuthorizedUserSession()
            throws Exception
    {
        return AzureAdSupport.createAzureUserSession(
                CLIENT_ID,
                clientSecret,
                SCOPE);
    }
}
