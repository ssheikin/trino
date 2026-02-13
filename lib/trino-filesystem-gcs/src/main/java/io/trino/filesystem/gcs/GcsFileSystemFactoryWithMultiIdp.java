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
package io.trino.filesystem.gcs;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.base.security.passthrough.IdPName;
import io.trino.plugin.base.security.passthrough.TokenPassThroughConfig;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Map;
import java.util.Optional;

import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_OAUTH_TOKEN_PROPERTY;
import static io.trino.plugin.base.security.passthrough.TokenPassThrough.getToken;
import static java.util.Objects.requireNonNull;

public class GcsFileSystemFactoryWithMultiIdp
        implements TrinoFileSystemFactory
{
    private final TrinoFileSystemFactory delegate;
    private final Optional<IdPName> idpName;

    @Inject
    public GcsFileSystemFactoryWithMultiIdp(@ForMultiIdp TrinoFileSystemFactory delegate, TokenPassThroughConfig tokenPassThroughConfig)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.idpName = tokenPassThroughConfig.getIdpName();
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        Map<String, String> extraCredentials = ImmutableMap.<String, String>builder()
                .putAll(identity.getExtraCredentials())
                .put(EXTRA_CREDENTIALS_OAUTH_TOKEN_PROPERTY, getToken(identity, idpName))
                .buildKeepingLast();

        return delegate.create(ConnectorIdentity
                .forUser(identity.getUser())
                .withConnectorRole(identity.getConnectorRole())
                .withEnabledSystemRoles(identity.getEnabledSystemRoles())
                .withGroups(identity.getGroups())
                .withPrincipal(identity.getPrincipal())
                .withExtraCredentials(extraCredentials)
                .build());
    }
}
