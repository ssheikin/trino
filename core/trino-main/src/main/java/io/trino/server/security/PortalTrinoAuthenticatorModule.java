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
package io.trino.server.security;

import com.google.common.collect.Iterables;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.client.HttpClient;
import io.airlift.node.NodeInfo;
import io.starburst.stargate.security.http.JwksHttpClient;
import io.trino.node.AnnounceNodeAnnouncerConfig;
import io.trino.server.starburst.security.GalaxyAuthenticatorController;
import io.trino.server.starburst.security.GalaxyTrinoAuthenticator;
import io.trino.server.starburst.security.JwtClaimsParser;

import java.net.URI;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;

public class PortalTrinoAuthenticatorModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        httpClientBinder(binder).bindHttpClient("portal-jwks", ForPortalAuthenticator.class);

        newOptionalBinder(binder, PortalAuthenticator.class)
                .setBinding()
                .to(GalaxyTrinoAuthenticator.class);
    }

    @Provides
    @Singleton
    public static JwksHttpClient createJwksHttpClient(@ForPortalAuthenticator HttpClient httpClient)
    {
        return new JwksHttpClient(httpClient);
    }

    @Provides
    @Singleton
    @ForPortalAuthenticator
    public static JwtClaimsParser createPortalJwtClaimsParser(
            JwksHttpClient jwksHttpClient,
            AnnounceNodeAnnouncerConfig announcerConfig,
            NodeInfo nodeInfo)
    {
        URI portalUri = getPortalUri(announcerConfig);
        URI jwksUri = uriBuilderFrom(portalUri).appendPath("/.well-known/jwks.json").build();
        return new PortalJwtClaimsParser(jwksHttpClient, jwksUri, nodeInfo.getEnvironment());
    }

    @Provides
    @Singleton
    public static GalaxyAuthenticatorController createGalaxyAuthenticatorController(
            @ForPortalAuthenticator JwtClaimsParser jwtClaimsParser,
            AnnounceNodeAnnouncerConfig announcerConfig)
    {
        URI portalUri = getPortalUri(announcerConfig);
        return new GalaxyAuthenticatorController(portalUri.toString(), jwtClaimsParser);
    }

    private static URI getPortalUri(AnnounceNodeAnnouncerConfig announcerConfig)
    {
        List<URI> coordinatorUris = announcerConfig.getCoordinatorUris();
        checkState(coordinatorUris.size() == 1, "Only one discovery uri is allowed");
        return Iterables.getOnlyElement(coordinatorUris);
    }
}
