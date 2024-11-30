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
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.azure.HiveAzureConfig;
import io.trino.plugin.base.security.passthrough.IdPName;
import io.trino.plugin.base.security.passthrough.TokenPassThroughConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.TestingConnectorSession;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URISyntaxException;

import static io.trino.hdfs.ConfigurationUtils.newEmptyConfiguration;
import static io.trino.hdfs.azure.passthrough.TrinoAzureAdConfigurationUpdater.TRINO_INTERNAL_ACCESS_TOKEN;
import static io.trino.plugin.base.security.passthrough.MultipleTokensPassthrough.MULTIPLE_TOKENS_KEY_PREFIX;
import static io.trino.plugin.base.security.passthrough.OAuth2TokenPassThrough.OAUTH2_ACCESS_TOKEN_PASSTHROUGH_CREDENTIAL;
import static java.util.Map.entry;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTrinoAzureAdConfigurationUpdater
{
    @Test
    public void testThrowsWhenNoUserToken()
    {
        assertThatThrownBy(() -> new TrinoAzureAdConfigurationUpdater(new TokenPassThroughConfig())
                .updateConfiguration(
                        newEmptyConfiguration(),
                        noTokenHdfsContext(),
                        dummyUri()))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Token pass-through authentication requires a valid token, but none has been found");
    }

    @Test
    public void testShouldSetTokenFromIdentity()
    {
        String token = "token";
        Configuration configuration = newEmptyConfiguration();

        processConfiguration(
                new TrinoAzureAdConfigurationUpdater(new TokenPassThroughConfig()),
                hdfsContextWithToken(token),
                configuration);

        assertThat(configuration)
                .contains(
                        entry(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, AuthType.Custom.name()),
                        entry(FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME, TrinoAzureAdTokenProvider.class.getName()),
                        entry(TRINO_INTERNAL_ACCESS_TOKEN, token));

        TrinoAzureAdTokenProvider configuredTokenProvider = new TrinoAzureAdTokenProvider();
        configuredTokenProvider.initialize(configuration, "account");

        assertThat(configuredTokenProvider.getAccessToken()).isEqualTo(token);
    }

    @Test
    public void testShouldSetTokenFromIdentityMultiIdp()
    {
        String token = "token";
        IdPName idPName = IdPName.of("idp");
        Configuration configuration = newEmptyConfiguration();
        TokenPassThroughConfig tokenPassThroughConfig = new TokenPassThroughConfig();
        tokenPassThroughConfig.setIdpName(idPName.toString());

        processConfiguration(
                new TrinoAzureAdConfigurationUpdater(tokenPassThroughConfig),
                hdfsContextWithTokenMultiIdp(token, idPName),
                configuration);

        assertThat(configuration)
                .contains(
                        entry(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, AuthType.Custom.name()),
                        entry(FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME, TrinoAzureAdTokenProvider.class.getName()),
                        entry(TRINO_INTERNAL_ACCESS_TOKEN, token));

        TrinoAzureAdTokenProvider configuredTokenProvider = new TrinoAzureAdTokenProvider();
        configuredTokenProvider.initialize(configuration, "account");

        assertThat(configuredTokenProvider.getAccessToken()).isEqualTo(token);
    }

    private void processConfiguration(TrinoAzureAdConfigurationUpdater processor, HdfsContext context, Configuration toProcess)
    {
        new TrinoAzureAdConfigurationInitializer(new HiveAzureConfig()).initializeConfiguration(toProcess);
        processor.updateConfiguration(toProcess, context, dummyUri());
    }

    private HdfsContext noTokenHdfsContext()
    {
        return new HdfsContext(TestingConnectorSession.SESSION);
    }

    private HdfsContext hdfsContextWithToken(String token)
    {
        return new HdfsContext(
                ConnectorIdentity.forUser("user")
                        .withExtraCredentials(ImmutableMap.of(OAUTH2_ACCESS_TOKEN_PASSTHROUGH_CREDENTIAL, token))
                        .build());
    }

    private HdfsContext hdfsContextWithTokenMultiIdp(String token, IdPName idpName)
    {
        return new HdfsContext(
                ConnectorIdentity.forUser("user")
                        .withExtraCredentials(ImmutableMap.of(MULTIPLE_TOKENS_KEY_PREFIX + idpName.toString(), token))
                        .build());
    }

    private URI dummyUri()
    {
        try {
            return new URI("https://example.com");
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
