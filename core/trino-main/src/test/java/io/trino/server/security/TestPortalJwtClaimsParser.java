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

import com.google.common.collect.ImmutableList;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.json.JsonCodec;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import io.starburst.stargate.authentication.JwksKeys;
import io.starburst.stargate.authentication.JwksRsaKey;
import io.starburst.stargate.security.http.JwksHttpClient;
import io.trino.plugin.base.util.AutoCloseableCloser;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.net.URI;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPublicKey;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.server.security.jwt.JwtUtil.newJwtBuilder;
import static io.trino.testing.assertions.Assert.assertEventually;
import static jakarta.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
import static java.time.Instant.now;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestPortalJwtClaimsParser
{
    private static final JsonCodec<JwksKeys> JWKS_CODEC = jsonCodec(JwksKeys.class);
    private static final String KEY_ID = "test-key";
    private static final String SUBJECT = "test-subject";

    private final KeyPair keyPair = generateRsaKeyPair();
    private final JwksServlet jwksServlet = new JwksServlet();

    @AutoClose
    private final AutoCloseableCloser closer = AutoCloseableCloser.create();

    private TestingHttpServer jwksServer;
    private JettyHttpClient httpClient;
    private URI jwksUri;

    @BeforeAll
    public void setUp()
            throws Exception
    {
        jwksServlet.setBody(jwksResponse(keyPair));
        NodeInfo nodeInfo = new NodeInfo("test");
        HttpServerConfig config = new HttpServerConfig().setHttpPort(0);
        jwksServer = new TestingHttpServer("testing-jwks-server", new HttpServerInfo(config, nodeInfo), nodeInfo, config, jwksServlet);
        jwksServer.start();
        closer.register(jwksServer::stop);
        jwksUri = jwksServer.getBaseUrl();
        httpClient = closer.register(new JettyHttpClient());
    }

    @BeforeEach
    public void resetServlet()
    {
        jwksServlet.setBody(jwksResponse(keyPair));
    }

    @Test
    public void testValidToken()
    {
        PortalJwtClaimsParser parser = createParser();
        Claims claims = parser.claims(buildToken(keyPair, KEY_ID, SUBJECT, expireIn(1)));
        assertThat(claims.getSubject()).isEqualTo(SUBJECT);
    }

    @Test
    public void testWrongKeyRejected()
    {
        PortalJwtClaimsParser parser = createParser();
        KeyPair wrongPair = generateRsaKeyPair();
        assertThatThrownBy(() -> parser.claims(buildToken(wrongPair, KEY_ID, SUBJECT, expireIn(1))))
                .isInstanceOf(JwtException.class)
                .hasMessage("JWT signature does not match locally computed signature. JWT validity cannot be asserted and should not be trusted.");
    }

    @Test
    public void testExpiredTokenRejected()
    {
        PortalJwtClaimsParser parser = createParser();
        assertThatThrownBy(() -> parser.claims(buildToken(keyPair, KEY_ID, SUBJECT, expireIn(-1))))
                .isInstanceOf(JwtException.class)
                .hasMessageMatching("JWT expired \\d+ milliseconds ago at .+\\. Current time: .+\\. Allowed clock skew: 0 milliseconds\\.");
    }

    @Test
    public void testWrongSubjectRejected()
    {
        PortalJwtClaimsParser parser = createParser();
        assertThatThrownBy(() -> parser.claims(buildToken(keyPair, KEY_ID, "wrong-subject", expireIn(1))))
                .isInstanceOf(JwtException.class)
                .hasMessage("Expected sub claim to be: test-subject, but was: wrong-subject.");
    }

    @Test
    public void testMissingKeyIdRejected()
    {
        PortalJwtClaimsParser parser = createParser();
        Date issuedAt = Date.from(now());
        String token = newJwtBuilder()
                .subject(SUBJECT)
                .expiration(expireIn(1))
                .issuedAt(issuedAt)
                .signWith(keyPair.getPrivate())
                .compact();
        assertThatThrownBy(() -> parser.claims(token))
                .isInstanceOf(JwtException.class)
                .hasMessage("Key ID is required");
    }

    @Test
    public void testUnknownKeyIdRejected()
    {
        PortalJwtClaimsParser parser = createParser();
        assertThatThrownBy(() -> parser.claims(buildToken(keyPair, "unknown-kid", SUBJECT, expireIn(1))))
                .isInstanceOf(JwtException.class)
                .hasMessage("Unknown signing key ID: unknown-kid");
    }

    @Test
    public void testRefreshPicksUpNewKeys()
    {
        KeyPair newPair = generateRsaKeyPair();
        PortalJwtClaimsParser parser = createParser();

        assertThat(parser.claims(buildToken(keyPair, KEY_ID, SUBJECT, expireIn(1))).getSubject())
                .isEqualTo(SUBJECT);

        jwksServlet.setBody(jwksResponse(newPair));
        parser.refreshKeys();

        assertThat(parser.claims(buildToken(newPair, KEY_ID, SUBJECT, expireIn(1))).getSubject())
                .isEqualTo(SUBJECT);
        assertThatThrownBy(() -> parser.claims(buildToken(keyPair, KEY_ID, SUBJECT, expireIn(1))))
                .isInstanceOf(JwtException.class)
                .hasMessage("JWT signature does not match locally computed signature. JWT validity cannot be asserted and should not be trusted.");
    }

    @Test
    public void testRequestFailurePreservesKeys()
    {
        PortalJwtClaimsParser parser = createParser();

        jwksServlet.setError();
        assertThatThrownBy(parser::refreshKeys)
                .isInstanceOf(RuntimeException.class)
                .hasMessageMatching("Error fetching portal JWKS keys from http://.+:\\d+");

        assertThat(parser.claims(buildToken(keyPair, KEY_ID, SUBJECT, expireIn(1))).getSubject())
                .isEqualTo(SUBJECT);
    }

    @Test
    public void testStartStopIsIdempotent()
    {
        PortalJwtClaimsParser parser = createParser();
        parser.start();
        parser.start();
        parser.stop();
        parser.stop();
    }

    @Test
    public void testTimedRefresh()
    {
        KeyPair newPair = generateRsaKeyPair();
        PortalJwtClaimsParser parser = new PortalJwtClaimsParser(
                new JwksHttpClient(httpClient),
                jwksUri,
                SUBJECT,
                new Duration(1, SECONDS));
        parser.start();
        try {
            jwksServlet.setBody(jwksResponse(newPair));
            String newToken = buildToken(newPair, KEY_ID, SUBJECT, expireIn(1));
            assertEventually(
                    new Duration(5, SECONDS),
                    () -> assertThat(parser.claims(newToken).getSubject()).isEqualTo(SUBJECT));
        }
        finally {
            parser.stop();
        }
    }

    private PortalJwtClaimsParser createParser()
    {
        return new PortalJwtClaimsParser(
                new JwksHttpClient(httpClient),
                jwksUri,
                SUBJECT,
                new Duration(1, DAYS));
    }

    private static String buildToken(KeyPair keyPair, String keyId, String subject, Date expiration)
    {
        Date issuedAt = Date.from(now());
        return newJwtBuilder()
                .header().keyId(keyId).and()
                .subject(subject)
                .expiration(expiration)
                .issuedAt(issuedAt)
                .signWith(keyPair.getPrivate())
                .compact();
    }

    private static Date expireIn(int hours)
    {
        return Date.from(now().plus(hours, ChronoUnit.HOURS));
    }

    private static String jwksResponse(KeyPair keyPair)
    {
        RSAPublicKey pub = (RSAPublicKey) keyPair.getPublic();
        JwksKeys keys = new JwksKeys(ImmutableList.of(new JwksRsaKey(KEY_ID, pub.getPublicExponent(), pub.getModulus())));
        return JWKS_CODEC.toJson(keys);
    }

    private static KeyPair generateRsaKeyPair()
    {
        try {
            KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
            generator.initialize(2048);
            return generator.generateKeyPair();
        }
        catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private static class JwksServlet
            extends HttpServlet
    {
        private final AtomicReference<String> body = new AtomicReference<>();

        public void setBody(String jwksJson)
        {
            body.set(jwksJson);
        }

        public void setError()
        {
            body.set(null);
        }

        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
                throws IOException
        {
            String jwks = body.get();
            if (jwks == null) {
                response.sendError(SC_INTERNAL_SERVER_ERROR);
                return;
            }
            response.setContentType("application/json");
            response.getWriter().println(jwks);
        }
    }
}
