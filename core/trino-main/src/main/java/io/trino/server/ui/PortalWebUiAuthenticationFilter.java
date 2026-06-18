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
package io.trino.server.ui;

import com.google.inject.Inject;
import io.trino.server.security.AuthenticationException;
import io.trino.server.starburst.security.GalaxyAuthenticatorController;
import io.trino.spi.security.Identity;
import jakarta.ws.rs.container.ContainerRequestContext;

import java.util.List;
import java.util.Optional;

import static io.trino.server.ServletSecurityUtils.sendWwwAuthenticate;
import static io.trino.server.ServletSecurityUtils.setAuthenticatedIdentity;
import static io.trino.server.starburst.security.GalaxyAuthenticationHelper.extractToken;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

public class PortalWebUiAuthenticationFilter
        implements WebUiAuthenticationFilter
{
    private final GalaxyAuthenticatorController controller;

    @Inject
    public PortalWebUiAuthenticationFilter(GalaxyAuthenticatorController controller)
    {
        this.controller = requireNonNull(controller, "controller is null");
    }

    @Override
    public void filter(ContainerRequestContext request)
    {
        try {
            Optional<String> token = extractToken(request);
            if (token.isEmpty()) {
                sendWwwAuthenticate(request, "Galaxy token is required", List.of("Galaxy"));
                return;
            }
            Identity identity = controller.authenticate(token.get(), Optional.empty());
            setAuthenticatedIdentity(request, identity);
        }
        catch (AuthenticationException e) {
            sendWwwAuthenticate(
                    request,
                    requireNonNullElse(e.getMessage(), "Unauthorized"),
                    e.getAuthenticateHeader().map(List::of).orElse(List.of()));
        }
    }
}
