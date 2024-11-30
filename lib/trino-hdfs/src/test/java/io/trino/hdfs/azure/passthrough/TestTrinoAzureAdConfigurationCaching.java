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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfiguration;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.hdfs.azure.HiveAzureConfig;
import io.trino.hdfs.azure.TrinoAzureConfigurationInitializer;
import io.trino.plugin.base.security.passthrough.TokenPassThroughConfig;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static io.trino.hdfs.azure.passthrough.TrinoAzureAdConfigurationUpdater.TRINO_INTERNAL_ACCESS_TOKEN;
import static io.trino.plugin.base.security.passthrough.OAuth2TokenPassThrough.OAUTH2_ACCESS_TOKEN_PASSTHROUGH_CREDENTIAL;
import static java.lang.String.format;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTrinoAzureAdConfigurationCaching
{
    private static final HdfsConfig HDFS_CONFIG = new HdfsConfig();

    private static final String STORAGE_ACCOUNT = "storageAccount";
    private static final Path ADLS_PATH = new Path(format("abfs://%s@%s.dfs.core.windows.net/", "testcontainer", STORAGE_ACCOUNT));

    @Test
    public void testCachesFileSystem()
            throws IOException
    {
        HdfsEnvironment testedEnvironment = new HdfsEnvironment(
                hdfsConfiguration(),
                HDFS_CONFIG,
                new NoHdfsAuthentication());

        Configuration userConfiguration = getConfForUser(testedEnvironment, passthroughUser("user"));
        Configuration otherUserConfiguration = getConfForUser(testedEnvironment, passthroughUser("other"));

        assertThat(userConfiguration.get(TRINO_INTERNAL_ACCESS_TOKEN))
                .isNotEqualTo(otherUserConfiguration.get(TRINO_INTERNAL_ACCESS_TOKEN));

        FileSystem firstCachedFileSystem = FileSystem.get(ADLS_PATH.toUri(), userConfiguration);

        assertThat(firstCachedFileSystem.getConf()).isSameAs(userConfiguration);
        assertThat(firstCachedFileSystem).isSameAs(FileSystem.get(ADLS_PATH.toUri(), userConfiguration));
        assertThat(FileSystem.get(ADLS_PATH.toUri(), otherUserConfiguration).getConf()).isSameAs(otherUserConfiguration);
        assertThat(firstCachedFileSystem).isNotSameAs(FileSystem.get(ADLS_PATH.toUri(), userConfiguration));
    }

    private Configuration getConfForUser(HdfsEnvironment hdfsEnvironment, ConnectorIdentity identity)
            throws IOException
    {
        return hdfsEnvironment.getFileSystem(new HdfsContext(identity), ADLS_PATH).getConf();
    }

    private HdfsConfiguration hdfsConfiguration()
    {
        HiveAzureConfig baseTrinoConfig = new HiveAzureConfig();
        return new DynamicHdfsConfiguration(
                new HdfsConfigurationInitializer(
                        HDFS_CONFIG,
                        ImmutableSet.of(
                                new TrinoAzureConfigurationInitializer(baseTrinoConfig),
                                new TrinoAzureAdConfigurationInitializer(baseTrinoConfig))),
                ImmutableSet.of(new TrinoAzureAdConfigurationUpdater(new TokenPassThroughConfig())));
    }

    private ConnectorIdentity passthroughUser(String username)
    {
        return ConnectorIdentity.forUser(username)
                .withExtraCredentials(
                        ImmutableMap.of(
                                OAUTH2_ACCESS_TOKEN_PASSTHROUGH_CREDENTIAL,
                                randomUUID().toString()))
                .build();
    }
}
