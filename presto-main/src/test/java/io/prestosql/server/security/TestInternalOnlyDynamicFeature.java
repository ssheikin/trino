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
package io.prestosql.server.security;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.inject.Key;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.HttpRequestFilter;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.http.server.HttpServerInfo;
import io.prestosql.server.InternalAuthenticationManager;
import io.prestosql.server.testing.TestingPrestoServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.testing.Closeables.closeQuietly;
import static javax.servlet.http.HttpServletResponse.SC_FORBIDDEN;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.testng.Assert.assertEquals;

public class TestInternalOnlyDynamicFeature
{
    private static final String LOCALHOST_KEYSTORE = Resources.getResource("cert/localhost.pem").getPath();
    private static final ImmutableMap<String, String> SECURE_PROPERTIES = ImmutableMap.<String, String>builder()
            .put("http-server.https.enabled", "true")
            .put("http-server.https.keystore.path", LOCALHOST_KEYSTORE)
            .put("http-server.https.keystore.key", "")
            .build();

    private JettyHttpClient client;

    @BeforeClass
    public void setup()
    {
        client = new JettyHttpClient(new HttpClientConfig()
                .setTrustStorePath(LOCALHOST_KEYSTORE)
                .setTrustStorePassword("")
                .setKeyStorePath(LOCALHOST_KEYSTORE)
                .setKeyStorePassword(""));
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        closeQuietly(client);
        client = null;
    }

    @Test
    public void testInternalOnlyChecks()
            throws Exception
    {
        try (TestingPrestoServer server = TestingPrestoServer.builder()
                .setProperties(SECURE_PROPERTIES)
                .build()) {
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));

            // normal resources should work
            assertResponseCode(getLocation(httpServerInfo.getHttpUri(), "/v1/info"), SC_OK);
            assertResponseCode(getLocation(httpServerInfo.getHttpsUri(), "/v1/info"), SC_OK);

            // task resource should be forbidden
            assertResponseCode(getLocation(httpServerInfo.getHttpUri(), "/v1/task"), SC_FORBIDDEN);
            assertResponseCode(getLocation(httpServerInfo.getHttpsUri(), "/v1/task"), SC_FORBIDDEN);

            // task resource with internal token should work
            HttpRequestFilter internalRequestFilter = server.getInstance(Key.get(InternalAuthenticationManager.class));
            assertResponseCode(getLocation(httpServerInfo.getHttpUri(), "/v1/task"), SC_OK, Optional.of(internalRequestFilter));
            assertResponseCode(getLocation(httpServerInfo.getHttpsUri(), "/v1/task"), SC_OK, Optional.of(internalRequestFilter));
        }
    }

    @Test
    public void testInternalOnlyCheckDisabled()
            throws Exception
    {
        try (TestingPrestoServer server = TestingPrestoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .putAll(SECURE_PROPERTIES)
                        .put("internal-only-checks.enabled", "false")
                        .build())
                .build()) {
            HttpServerInfo httpServerInfo = server.getInstance(Key.get(HttpServerInfo.class));

            // all resources should work
            assertResponseCode(getLocation(httpServerInfo.getHttpUri(), "/v1/info"), SC_OK);
            assertResponseCode(getLocation(httpServerInfo.getHttpsUri(), "/v1/info"), SC_OK);
            assertResponseCode(getLocation(httpServerInfo.getHttpUri(), "/v1/task"), SC_OK);
            assertResponseCode(getLocation(httpServerInfo.getHttpsUri(), "/v1/task"), SC_OK);
        }
    }

    private void assertResponseCode(URI uri, int expectedCode)
    {
        assertResponseCode(uri, expectedCode, Optional.empty());
    }

    private void assertResponseCode(URI uri, int expectedCode, Optional<HttpRequestFilter> requestFilter)
    {
        Request request = prepareGet().setUri(uri).build();
        if (requestFilter.isPresent()) {
            request = requestFilter.get().filterRequest(request);
        }

        StatusResponse statusResponse = client.execute(request, createStatusResponseHandler());
        assertEquals(statusResponse.getStatusCode(), expectedCode);
    }

    private static URI getLocation(URI baseUri, String path)
    {
        return uriBuilderFrom(baseUri).replacePath(path).build();
    }
}
