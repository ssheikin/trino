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
package io.trino.server.security.oauth2;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.trino.server.ExternalUriInfo;
import io.trino.server.security.ResourceSecurity;
import io.trino.server.security.oauth2.OAuth2TokenExchange.TokenPoll;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.BeanParam;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.CookieParam;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Cookie;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.jaxrs.AsyncResponseHandler.bindAsyncResponse;
import static io.trino.server.AsyncResponseUtils.withFallbackAfterTimeout;
import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;
import static io.trino.server.security.oauth2.CsrfTokenCookie.CSRF_COOKIE;
import static io.trino.server.security.oauth2.OAuth2CallbackResource.CALLBACK_ENDPOINT;
import static io.trino.server.security.oauth2.OAuth2Service.generateCsrfToken;
import static io.trino.server.security.oauth2.OAuth2TokenExchange.MAX_POLL_TIME;
import static io.trino.server.security.oauth2.OAuth2TokenExchange.hashAuthId;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static java.util.Objects.requireNonNull;

@Path(OAuth2TokenExchangeResource.TOKEN_ENDPOINT)
@ResourceSecurity(PUBLIC)
public class OAuth2TokenExchangeResource
{
    public static final String CSRF_HIDDEN_FORM_FIELD = "_csrf";
    static final String TOKEN_ENDPOINT = "/oauth2/token/";

    private static final JsonCodec<Map<String, Object>> MAP_CODEC = new JsonCodecFactory().mapJsonCodec(String.class, Object.class);

    private final OAuth2TokenExchange tokenExchange;
    private final OAuth2Service service;
    private final Executor responseExecutor;
    private final ScheduledExecutorService timeoutExecutor;

    @Inject
    public OAuth2TokenExchangeResource(OAuth2TokenExchange tokenExchange, OAuth2Service service, @ForOAuth2 ExecutorService responseExecutor, @ForOAuth2 ScheduledExecutorService timeoutExecutor)
    {
        this.tokenExchange = requireNonNull(tokenExchange, "tokenExchange is null");
        this.service = requireNonNull(service, "service is null");
        this.responseExecutor = requireNonNull(responseExecutor, "responseExecutor is null");
        this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");
    }

    @Path("initiate/{authIdHash}")
    @GET
    @Produces(MediaType.TEXT_HTML)
    public Response initiateTokenExchange(@PathParam("authIdHash") String authIdHash)
    {
        // authIdHash implicitly propagated via action=""
        // the browser POSTs to the same URL it's currently on (e.g. /oauth2/token/initiate/abc123),
        // so the authIdHash path segment is needed here on GET to be preserved on POST later
        String csrfToken = generateCsrfToken();
        return Response.ok(service.getConfirmHtml(csrfToken), MediaType.TEXT_HTML)
                .cookie(CsrfTokenCookie.create(csrfToken))
                .build();
    }

    @Path("initiate/{authIdHash}")
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.APPLICATION_JSON)
    public Response confirmTokenExchange(
            @PathParam("authIdHash") String authIdHash,
            @BeanParam ExternalUriInfo externalUriInfo,
            @FormParam(CSRF_HIDDEN_FORM_FIELD) String csrfFormToken,
            @CookieParam(CSRF_COOKIE) Cookie csrfCookie)
    {
        String csrfCookieValue = CsrfTokenCookie.read(csrfCookie)
                .orElseThrow(() -> new ForbiddenException("Missing CSRF token"));
        if (!csrfCookieValue.equals(csrfFormToken)) {
            throw new ForbiddenException("Invalid CSRF token");
        }
        Response response = service.startOAuth2Challenge(externalUriInfo.absolutePath(CALLBACK_ENDPOINT), Optional.ofNullable(authIdHash));
        return Response.fromResponse(response)
                .cookie(CsrfTokenCookie.delete())
                .build();
    }

    @Path("{authId}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public void getAuthenticationToken(@PathParam("authId") UUID authId, @Suspended AsyncResponse asyncResponse, @Context HttpServletRequest request)
    {
        if (authId == null) {
            throw new BadRequestException();
        }

        // Do not drop the response from the cache on failure, as this would result in a
        // hang if the client retries the request. The response will timeout eventually.
        ListenableFuture<TokenPoll> tokenFuture = tokenExchange.getTokenPoll(authId);
        ListenableFuture<Response> responseFuture = withFallbackAfterTimeout(
                transform(tokenFuture, OAuth2TokenExchangeResource::toResponse, directExecutor()),
                MAX_POLL_TIME,
                () -> pendingResponse(request),
                timeoutExecutor);
        bindAsyncResponse(asyncResponse, responseFuture, responseExecutor);
    }

    private static Response toResponse(TokenPoll poll)
    {
        if (poll.getError().isPresent()) {
            return Response.ok(jsonMap("error", poll.getError().get()), APPLICATION_JSON_TYPE).build();
        }
        if (poll.getToken().isPresent()) {
            return Response.ok(jsonMap("token", poll.getToken().get()), APPLICATION_JSON_TYPE).build();
        }
        throw new VerifyException("invalid TokenPoll state");
    }

    private static Response pendingResponse(HttpServletRequest request)
    {
        return Response.ok(jsonMap("nextUri", request.getRequestURL()), APPLICATION_JSON_TYPE).build();
    }

    @DELETE
    @Path("{authId}")
    public Response deleteAuthenticationToken(@PathParam("authId") UUID authId)
    {
        if (authId == null) {
            throw new BadRequestException();
        }

        tokenExchange.dropToken(authId);
        return Response
                .ok()
                .build();
    }

    public static String getTokenUri(UUID authId)
    {
        return TOKEN_ENDPOINT + authId;
    }

    public static String getInitiateUri(UUID authId)
    {
        return TOKEN_ENDPOINT + "initiate/" + hashAuthId(authId);
    }

    private static String jsonMap(String key, Object value)
    {
        return MAP_CODEC.toJson(ImmutableMap.of(key, value));
    }
}
