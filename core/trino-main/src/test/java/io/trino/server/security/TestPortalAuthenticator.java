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
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import com.google.common.io.Resources;
import com.google.inject.Key;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.json.JsonCodec;
import io.airlift.node.NodeInfo;
import io.starburst.stargate.authentication.JwksKeys;
import io.starburst.stargate.authentication.JwksRsaKey;
import io.starburst.stargate.token.SigningKey;
import io.trino.server.testing.TestingTrinoServer;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.net.URI;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.interfaces.RSAPublicKey;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;

import static com.google.common.hash.Hashing.sha256;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.starburst.stargate.security.JwtUtil.newJwtBuilder;
import static io.starburst.stargate.token.generation.TokenGenerator.AT_JWT_TYPE;
import static io.trino.client.OkHttpUtil.setupSsl;
import static jakarta.servlet.http.HttpServletResponse.SC_OK;
import static jakarta.servlet.http.HttpServletResponse.SC_UNAUTHORIZED;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Instant.now;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestPortalAuthenticator
{
    private static final String LOCALHOST_KEYSTORE = Resources.getResource("cert/localhost.pem").getPath();
    private static final ImmutableMap<String, String> SECURE_PROPERTIES = ImmutableMap.<String, String>builder()
            .put("http-server.https.enabled", "true")
            .put("http-server.https.keystore.path", LOCALHOST_KEYSTORE)
            .put("http-server.https.keystore.key", "")
            .put("http-server.process-forwarded", "true")
            .buildOrThrow();
    private static final String TOKEN_HEADER_NAME = "X-Galaxy-Authentication";
    private static final String TEST_USER_NAME = "test-user";
    private static final String ACCOUNT_ID = "a-12345678";
    private static final String USER_ID = "u-1234567890";
    private static final String ROLE_ID = "r-9876543252";
    private static final MediaType PLAIN_TEXT_UTF_8 = MediaType.get("text/plain; charset=utf-8");
    private static final String PORTAL_STATEMENT_PATH = "/v1/statement/queued/test_query/test_slug";
    private static final String WEB_UI_IDENTITY_PATH = "/ui/api/identity";
    private static final KeyPair KEY_PAIR = generateRsaKeyPair();

    private final SigningKey signingKey = new SigningKey("global", KEY_PAIR.getPrivate());
    private final Closer closer = Closer.create();

    private OkHttpClient client;
    private URI baseUri;
    private URI portalUri;
    private String environment;

    @BeforeAll
    public void setup()
            throws Exception
    {
        TestingHttpServer jwksServer = createJwksServer(KEY_PAIR.getPublic());
        closer.register(() -> {
            try {
                jwksServer.stop();
            }
            catch (Exception e) {
                throw new IOException(e);
            }
        });
        jwksServer.start();
        portalUri = jwksServer.getBaseUrl();

        TestingTrinoServer trinoServer = closer.register(TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(SECURE_PROPERTIES)
                        .put("discovery.uri", portalUri.toString())
                        .put("http-server.authentication.type", "portal")
                        .buildOrThrow())
                .setAdditionalModule(binder -> jaxrsBinder(binder).bind(TestResourceSecurity.TestResource.class))
                .build());

        baseUri = trinoServer.getInstance(Key.get(HttpServerInfo.class)).getHttpsUri();
        environment = trinoServer.getInstance(Key.get(NodeInfo.class)).getEnvironment();

        OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder();
        setupSsl(
                clientBuilder,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                false,
                Optional.of(LOCALHOST_KEYSTORE),
                Optional.empty(),
                Optional.empty(),
                false);
        client = clientBuilder.build();
    }

    @AfterAll
    public void tearDown()
            throws IOException
    {
        closer.close();
    }

    @Test
    public void testPublicEndpointAccessibleWithoutToken()
            throws IOException
    {
        assertResponseCode(location("/v1/info"), SC_OK, Headers.of());
    }

    @Test
    public void testMissingTokenRejected()
            throws IOException
    {
        assertResponseCode(putStatementRequest("SELECT 1", Optional.empty()), SC_UNAUTHORIZED);
    }

    @Test
    public void testInvalidTokenRejected()
            throws IOException
    {
        assertTokenRejected("not-a-jwt");
    }

    @Test
    public void testWrongKeyRejected()
            throws IOException
    {
        SigningKey wrongKey = new SigningKey("global", generateRsaKeyPair().getPrivate());
        assertTokenRejected(buildToken(wrongKey, portalUri.toString(), environment, ACCOUNT_ID, expireIn(1), requestExpiry(), TEST_USER_NAME, Optional.empty()));
    }

    @Test
    public void testExpiredTokenRejected()
            throws IOException
    {
        assertTokenRejected(buildToken(signingKey, portalUri.toString(), environment, ACCOUNT_ID, expireIn(-1), requestExpiry(), TEST_USER_NAME, Optional.empty()));
    }

    @Test
    public void testWrongIssuerRejected()
            throws IOException
    {
        assertTokenRejected(buildToken(signingKey, "https://wrong-issuer.example.com", environment, ACCOUNT_ID, expireIn(1), requestExpiry(), TEST_USER_NAME, Optional.empty()));
    }

    @Test
    public void testWrongDeploymentRejected()
            throws IOException
    {
        assertTokenRejected(buildToken(signingKey, portalUri.toString(), "wrong-environment", ACCOUNT_ID, expireIn(1), requestExpiry(), TEST_USER_NAME, Optional.empty()));
    }

    @Test
    public void testMissingUsernameRejected()
            throws IOException
    {
        assertTokenRejected(buildToken(signingKey, portalUri.toString(), environment, ACCOUNT_ID, expireIn(1), requestExpiry(), null, Optional.empty()));
    }

    @Test
    public void testPutStatementWithMatchingBodyHashAuthenticates()
            throws IOException
    {
        String statement = "SELECT 1";
        assertResponseCode(putStatementRequest(statement, Optional.of(tokenWithStatementHash(statement))), SC_OK);
    }

    @Test
    public void testPutStatementWithWrongBodyHashRejected()
            throws IOException
    {
        String token = tokenWithStatementHash("SELECT 1");
        assertResponseCode(putStatementRequest("SELECT 2", Optional.of(token)), SC_UNAUTHORIZED);
    }

    @Test
    public void testPutStatementWithoutBodyHashRejected()
            throws IOException
    {
        assertResponseCode(putStatementRequest("SELECT 1", Optional.of(validToken())), SC_UNAUTHORIZED);
    }

    @Test
    public void testExpiredRequestRejected()
            throws IOException
    {
        String statement = "SELECT 1";
        String token = buildToken(signingKey, portalUri.toString(), environment, ACCOUNT_ID, expireIn(1), expireIn(-1), TEST_USER_NAME, Optional.of(sha256().hashString(statement, UTF_8).toString()));
        assertResponseCode(putStatementRequest(statement, Optional.of(token)), SC_UNAUTHORIZED);
    }

    @Test
    public void testWebUiMissingTokenRejected()
            throws IOException
    {
        assertResponseCode(location(WEB_UI_IDENTITY_PATH), SC_UNAUTHORIZED, Headers.of());
    }

    @Test
    public void testWebUiInvalidTokenRejected()
            throws IOException
    {
        assertResponseCode(location(WEB_UI_IDENTITY_PATH), SC_UNAUTHORIZED, Headers.of(TOKEN_HEADER_NAME, "not-a-jwt"));
    }

    @Test
    public void testWebUiExpiredTokenRejected()
            throws IOException
    {
        String token = buildToken(signingKey, portalUri.toString(), environment, ACCOUNT_ID, expireIn(-1), requestExpiry(), TEST_USER_NAME, Optional.empty());
        assertResponseCode(location(WEB_UI_IDENTITY_PATH), SC_UNAUTHORIZED, Headers.of(TOKEN_HEADER_NAME, token));
    }

    @Test
    public void testWebUiWrongKeyRejected()
            throws IOException
    {
        SigningKey wrongKey = new SigningKey("global", generateRsaKeyPair().getPrivate());
        String token = buildToken(wrongKey, portalUri.toString(), environment, ACCOUNT_ID, expireIn(1), requestExpiry(), TEST_USER_NAME, Optional.empty());
        assertResponseCode(location(WEB_UI_IDENTITY_PATH), SC_UNAUTHORIZED, Headers.of(TOKEN_HEADER_NAME, token));
    }

    @Test
    public void testWebUiValidTokenAuthenticatesUser()
            throws IOException
    {
        String token = validToken();
        Request request = new Request.Builder()
                .url(location(WEB_UI_IDENTITY_PATH))
                .header(TOKEN_HEADER_NAME, token)
                .build();
        try (Response response = client.newCall(request).execute()) {
            assertThat(response.code()).isEqualTo(SC_OK);
            assertThat(response.header("user")).isEqualTo(TEST_USER_NAME);
        }
    }

    private void assertTokenRejected(String token)
            throws IOException
    {
        assertResponseCode(putStatementRequest("SELECT 1", Optional.of(token)), SC_UNAUTHORIZED);
    }

    private Request putStatementRequest(String statement, Optional<String> token)
    {
        Request.Builder request = new Request.Builder()
                .url(location(PORTAL_STATEMENT_PATH))
                .put(RequestBody.create(statement, PLAIN_TEXT_UTF_8));
        token.ifPresent(tokenHeader -> request.header(TOKEN_HEADER_NAME, tokenHeader));
        return request.build();
    }

    private String tokenWithStatementHash(String statement)
    {
        return buildToken(signingKey, portalUri.toString(), environment, ACCOUNT_ID, expireIn(1), requestExpiry(), TEST_USER_NAME, Optional.of(sha256().hashString(statement, UTF_8).toString()));
    }

    private String validToken()
    {
        return buildToken(signingKey, portalUri.toString(), environment, ACCOUNT_ID, expireIn(1), requestExpiry(), TEST_USER_NAME, Optional.empty());
    }

    private String location(String path)
    {
        return uriBuilderFrom(baseUri).replacePath(path).toString();
    }

    private void assertResponseCode(String url, int expectedCode, Headers headers)
            throws IOException
    {
        Request request = new Request.Builder()
                .url(url)
                .headers(headers)
                .build();
        assertResponseCode(request, expectedCode);
    }

    private void assertResponseCode(Request request, int expectedCode)
            throws IOException
    {
        try (Response response = client.newCall(request).execute()) {
            assertThat(response.code())
                    .describedAs(request.url().toString())
                    .isEqualTo(expectedCode);
        }
    }

    private static Date expireIn(int hours)
    {
        return Date.from(now().plus(hours, ChronoUnit.HOURS));
    }

    private static Date requestExpiry()
    {
        return Date.from(now().plusSeconds(60));
    }

    private static String buildToken(
            SigningKey signingKey,
            String issuer,
            String subject,
            String audience,
            Date expiration,
            Date requestExpiration,
            String username,
            Optional<String> statementHash)
    {
        requireNonNull(signingKey, "signingKey is null");
        Date issuedAt = Date.from(now());
        return newJwtBuilder()
                .header().keyId(signingKey.keyId()).type(AT_JWT_TYPE)
                .and()
                .issuer(issuer)
                .audience().add(audience)
                .and()
                .subject(subject)
                .expiration(expiration)
                .issuedAt(issuedAt)
                .notBefore(issuedAt)
                .id(UUID.randomUUID().toString())
                .claim("username", username)
                .claim("user_id", USER_ID)
                .claim("role_id", ROLE_ID)
                .claim("request_expiry", requestExpiration)
                .claim("statement_hash", statementHash.orElse(null))
                .signWith(signingKey.key())
                .compact();
    }

    private static TestingHttpServer createJwksServer(PublicKey publicKey)
            throws IOException
    {
        NodeInfo nodeInfo = new NodeInfo("test");
        HttpServerConfig config = new HttpServerConfig().setHttpPort(0);
        HttpServerInfo httpServerInfo = new HttpServerInfo(config, nodeInfo);
        return new TestingHttpServer("testing-jwks-server", httpServerInfo, nodeInfo, config, new JwksServlet(publicKey));
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
        private static final JsonCodec<JwksKeys> JWKS_KEYS_JSON_CODEC = jsonCodec(JwksKeys.class);

        private final String jwksResponse;

        public JwksServlet(PublicKey publicKey)
        {
            RSAPublicKey rsaPublicKey = (RSAPublicKey) publicKey;
            JwksKeys keys = new JwksKeys(ImmutableList.of(new JwksRsaKey("global", rsaPublicKey.getPublicExponent(), rsaPublicKey.getModulus())));
            this.jwksResponse = JWKS_KEYS_JSON_CODEC.toJson(keys);
        }

        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
                throws IOException
        {
            response.setContentType("application/json");
            response.getWriter().println(jwksResponse);
        }
    }
}
