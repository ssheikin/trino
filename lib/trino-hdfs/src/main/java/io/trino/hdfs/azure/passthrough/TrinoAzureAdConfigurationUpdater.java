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
import io.trino.hdfs.DynamicConfigurationProvider;
import io.trino.hdfs.HdfsContext;
import io.trino.plugin.base.security.passthrough.IdPName;
import io.trino.plugin.base.security.passthrough.TokenPassThroughConfig;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;
import java.util.Optional;

import static io.trino.hdfs.DynamicConfigurationProvider.setCacheKey;
import static io.trino.plugin.base.security.passthrough.TokenPassThrough.getToken;
import static java.util.Objects.requireNonNull;

public class TrinoAzureAdConfigurationUpdater
        implements DynamicConfigurationProvider
{
    public static final String TRINO_INTERNAL_ACCESS_TOKEN = "trino.azure.oauth.access-token";

    private final Optional<IdPName> idPName;

    @Inject
    public TrinoAzureAdConfigurationUpdater(TokenPassThroughConfig config)
    {
        requireNonNull(config, "config is null");
        this.idPName = requireNonNull(config.getIdpName(), "idpName is null");
    }

    @Override
    public void updateConfiguration(Configuration configuration, HdfsContext context, URI uri)
    {
        String accessToken = getToken(context.getIdentity(), idPName);
        configuration.set(TRINO_INTERNAL_ACCESS_TOKEN, accessToken);
        setCacheKey(configuration, accessToken);
    }
}
