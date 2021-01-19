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
package io.prestosql.server.security.oauth2;

import com.google.common.collect.Ordering;
import com.google.common.io.Resources;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.SigningKeyResolver;
import io.prestosql.server.security.oauth2.OAuth2Client.AccessToken;

import javax.inject.Inject;

import java.io.IOException;
import java.net.URI;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;

import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.hash.Hashing.sha256;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class OAuth2Service
{
    private static final String STATE_AUDIENCE_UI = "presto_oauth_ui";
    private static final String STATE_AUDIENCE_REST = "presto_oauth_rest";
    private static final String FAILURE_REPLACEMENT_TEXT = "<!-- ERROR_MESSAGE -->";

    private final OAuth2Client client;
    private final JwtParser jwtParser;

    private final String successHtml;
    private final String failureHtml;

    private final TemporalAmount challengeTimeout;
    private final byte[] stateHmac;

    @Inject
    public OAuth2Service(OAuth2Client client, @ForOAuth2 SigningKeyResolver signingKeyResolver, OAuth2Config oauth2Config)
            throws IOException
    {
        this.client = requireNonNull(client, "client is null");
        this.jwtParser = Jwts.parser().setSigningKeyResolver(signingKeyResolver);

        this.successHtml = Resources.toString(Resources.getResource(getClass(), "/oauth2/success.html"), UTF_8);
        this.failureHtml = Resources.toString(Resources.getResource(getClass(), "/oauth2/failure.html"), UTF_8);
        verify(failureHtml.contains(FAILURE_REPLACEMENT_TEXT), "login.html does not contain the replacement text");

        requireNonNull(oauth2Config, "oauth2Config is null");
        this.challengeTimeout = Duration.ofMillis(oauth2Config.getChallengeTimeout().toMillis());
        this.stateHmac = oauth2Config.getStateKey()
                .map(key -> sha256().hashString(key, UTF_8).asBytes())
                .orElseGet(() -> secureRandomBytes(32));
    }

    public URI startWebUiChallenge(URI callbackUri)
    {
        String state = Jwts.builder()
                .signWith(SignatureAlgorithm.HS256, stateHmac)
                .setAudience(STATE_AUDIENCE_UI)
                .setExpiration(Date.from(Instant.now().plus(challengeTimeout)))
                .compact();

        return client.getAuthorizationUri(state, callbackUri);
    }

    public URI startRestChallenge(URI callbackUri, UUID authId)
    {
        String state = Jwts.builder()
                .signWith(SignatureAlgorithm.HS256, stateHmac)
                .setId(authId.toString())
                .setAudience(STATE_AUDIENCE_REST)
                .setExpiration(Date.from(Instant.now().plus(challengeTimeout)))
                .compact();

        return client.getAuthorizationUri(state, callbackUri);
    }

    public OAuthResult finishChallenge(String state, String code, URI callbackUri)
            throws ChallengeFailedException
    {
        requireNonNull(callbackUri, "callbackUri is null");
        requireNonNull(state, "state is null");
        requireNonNull(code, "code is null");

        Claims stateClaims = parseState(state);
        Optional<UUID> authId;
        if (STATE_AUDIENCE_UI.equals(stateClaims.getAudience())) {
            authId = Optional.empty();
        }
        else if (STATE_AUDIENCE_REST.equals(stateClaims.getAudience())) {
            try {
                authId = Optional.of(UUID.fromString(stateClaims.getId()));
            }
            catch (IllegalArgumentException e) {
                throw new ChallengeFailedException("State is does not contain an auth ID");
            }
        }
        else {
            // this is very unlikely, but is a good safety check
            throw new ChallengeFailedException("Unexpected state audience");
        }

        // fetch access token
        AccessToken accessToken = client.getAccessToken(code, callbackUri);

        // validate access token is trusted by this server
        Claims parsedToken = jwtParser.parseClaimsJws(accessToken.getAccessToken()).getBody();

        // determine expiration
        Instant validUntil = accessToken.getValidUntil()
                .map(instant -> Ordering.natural().min(instant, parsedToken.getExpiration().toInstant()))
                .orElse(parsedToken.getExpiration().toInstant());

        return new OAuthResult(authId, accessToken.getAccessToken(), validUntil);
    }

    private Claims parseState(String state)
            throws ChallengeFailedException
    {
        try {
            return Jwts.parser()
                    .setSigningKey(stateHmac)
                    .parseClaimsJws(state)
                    .getBody();
        }
        catch (RuntimeException e) {
            throw new ChallengeFailedException("State validation failed", e);
        }
    }

    public Jws<Claims> parseClaimsJws(String token)
    {
        return jwtParser.parseClaimsJws(token);
    }

    public String getSuccessHtml()
    {
        return successHtml;
    }

    public String getCallbackErrorHtml(String errorCode)
    {
        return failureHtml.replace(FAILURE_REPLACEMENT_TEXT, getOAuth2ErrorMessage(errorCode));
    }

    public String getInternalFailureHtml(String errorMessage)
    {
        return failureHtml.replace(FAILURE_REPLACEMENT_TEXT, nullToEmpty(errorMessage));
    }

    private static byte[] secureRandomBytes(int count)
    {
        byte[] bytes = new byte[count];
        new SecureRandom().nextBytes(bytes);
        return bytes;
    }

    private static String getOAuth2ErrorMessage(String errorCode)
    {
        switch (errorCode) {
            case "access_denied":
                return "OAuth2 server denied the login";
            case "unauthorized_client":
                return "OAuth2 server does not allow request from this Presto server";
            case "server_error":
                return "OAuth2 server had a failure";
            case "temporarily_unavailable":
                return "OAuth2 server is temporarily unavailable";
            default:
                return "OAuth2 unknown error code: " + errorCode;
        }
    }

    public static class OAuthResult
    {
        private final Optional<UUID> authId;
        private final String accessToken;
        private final Instant tokenExpiration;

        public OAuthResult(Optional<UUID> authId, String accessToken, Instant tokenExpiration)
        {
            this.authId = requireNonNull(authId, "authId is null");
            this.accessToken = requireNonNull(accessToken, "accessToken is null");
            this.tokenExpiration = requireNonNull(tokenExpiration, "tokenExpiration is null");
        }

        /**
         * Get auth ID if this is a REST client request.
         * This will be empty if the authentication request is a web UI login.
         */
        public Optional<UUID> getAuthId()
        {
            return authId;
        }

        public String getAccessToken()
        {
            return accessToken;
        }

        public Instant getTokenExpiration()
        {
            return tokenExpiration;
        }
    }
}
