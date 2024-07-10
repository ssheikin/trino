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
import com.nimbusds.oauth2.sdk.ResourceOwnerPasswordCredentialsGrant;
import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.TokenRequest;
import com.nimbusds.oauth2.sdk.TokenResponse;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.ClientID;
import io.trino.Session;
import io.trino.spi.security.Identity;

import java.net.URI;

import static io.trino.filesystem.azure.AzureFileSystemConstants.OAUTH2_ACCESS_TOKEN_PASSTHROUGH_CREDENTIAL;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.lang.System.getenv;
import static java.util.Objects.requireNonNull;

final class AzureAdSupport
{
    private static final String AZURE_AD_SCOPE = "https://storage.azure.com/user_impersonation";
    private static final String AZURE_TENANT = "9ac50357-7ce0-4d4f-83d3-d8a10c328c05";
    private static final String AZURE_AD_TOKEN_URL = format("https://login.microsoftonline.com/%s/oauth2/v2.0/token", AZURE_TENANT);
    private static final String AZURE_AD_CLIENT_ID = "0de608dd-04e3-4957-83ce-956af59547c1";
    private static final String AZURE_AD_CLIENT_SECRET = requireNonNull(getenv("AZURE_AD_CLIENT_SECRET"), "AZURE_AD_CLIENT_SECRET environment variable is not set");

    static Session createDefaultUserSession()
            throws Exception
    {
        return createAzureUserSession(AZURE_AD_CLIENT_ID, AZURE_AD_CLIENT_SECRET, AZURE_AD_SCOPE);
    }

    static Session createAzureUserSession(String clientId, String clientSecret, String scope)
            throws Exception
    {
        String username = requireNonNull(getenv("AZURE_AD_USER"), "AZURE_AD_USER environment variable is not set");
        String password = requireNonNull(getenv("AZURE_AD_PASSWORD"), "AZURE_AD_PASSWORD environment variable is not set");
        return testSessionBuilder()
                .setIdentity(Identity.forUser(username)
                        .withAdditionalExtraCredentials(
                                ImmutableMap.of(
                                        OAUTH2_ACCESS_TOKEN_PASSTHROUGH_CREDENTIAL,
                                        accessTokenFor(username, password, clientId, clientSecret, scope)))
                        .build())
                .build();
    }

    private static String accessTokenFor(String username, String password, String clientId, String clientSecret, String scope)
            throws Exception
    {
        TokenResponse response = TokenResponse.parse(
                new TokenRequest(
                        URI.create(AZURE_AD_TOKEN_URL),
                        new ClientSecretBasic(new ClientID(clientId), new Secret(clientSecret)),
                        new ResourceOwnerPasswordCredentialsGrant(username, new Secret(password)),
                        new Scope(scope))
                        .toHTTPRequest().send());
        if (response.indicatesSuccess()) {
            return response.toSuccessResponse().getTokens().getAccessToken().getValue();
        }
        else {
            throw new RuntimeException(response.toErrorResponse().toJSONObject().toString());
        }
    }

    private AzureAdSupport() {}
}
