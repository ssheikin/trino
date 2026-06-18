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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import io.airlift.concurrent.Threads;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Header;
import io.jsonwebtoken.JweHeader;
import io.jsonwebtoken.JwsHeader;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Locator;
import io.jsonwebtoken.UnsupportedJwtException;
import io.jsonwebtoken.security.SecurityException;
import io.starburst.stargate.authentication.JwksRsaKey;
import io.starburst.stargate.security.http.JwksHttpClient;
import io.starburst.stargate.security.http.UnexpectedErrorException;
import io.trino.server.starburst.security.JwtClaimsParser;
import jakarta.annotation.Generated;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.io.IOException;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.PublicKey;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.server.security.jwt.JwtUtil.newJwtParserBuilder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public final class PortalJwtClaimsParser
        implements JwtClaimsParser
{
    private static final Logger log = Logger.get(PortalJwtClaimsParser.class);

    private final URI jwksUri;
    private final JwksHttpClient jwksHttpClient;
    private final Duration refreshDelay;
    private final AtomicReference<Map<String, PublicKey>> keys;
    private final JwtParser parser;

    @Generated("this")
    private Closer closer;

    public PortalJwtClaimsParser(JwksHttpClient jwksHttpClient, URI jwksUri, String deploymentId)
    {
        this(jwksHttpClient, jwksUri, deploymentId, new Duration(15, TimeUnit.MINUTES));
    }

    @VisibleForTesting
    PortalJwtClaimsParser(JwksHttpClient jwksHttpClient, URI jwksUri, String deploymentId, Duration refreshDelay)
    {
        this.jwksHttpClient = requireNonNull(jwksHttpClient, "jwksHttpClient is null");
        this.jwksUri = requireNonNull(jwksUri, "jwksUri is null");
        this.refreshDelay = requireNonNull(refreshDelay, "refreshDelay is null");
        requireNonNull(deploymentId, "deploymentId is null");
        this.keys = new AtomicReference<>(fetchKeys());
        this.parser = newJwtParserBuilder()
                .keyLocator(new KeyLocator(keys))
                .requireSubject(deploymentId)
                .build();
    }

    @PostConstruct
    public synchronized void start()
    {
        if (closer != null) {
            return;
        }
        closer = Closer.create();

        ScheduledExecutorService executorService = newSingleThreadScheduledExecutor(Threads.daemonThreadsNamed("portal-jwks-loader"));
        closer.register(executorService::shutdownNow);

        ScheduledFuture<?> refreshJob = executorService.scheduleWithFixedDelay(
                () -> {
                    try {
                        refreshKeys();
                    }
                    catch (Exception e) {
                        log.error(e, "Error refreshing portal JWKS keys from %s", jwksUri);
                    }
                },
                refreshDelay.toMillis(),
                refreshDelay.toMillis(),
                TimeUnit.MILLISECONDS);
        closer.register(() -> refreshJob.cancel(true));
    }

    @PreDestroy
    public synchronized void stop()
    {
        if (closer == null) {
            return;
        }
        try {
            closer.close();
        }
        catch (IOException e) {
            throw new RuntimeException("Error stopping portal JWKS service", e);
        }
        finally {
            closer = null;
        }
    }

    @VisibleForTesting
    void refreshKeys()
    {
        keys.set(fetchKeys());
    }

    @Override
    public Claims claims(String token)
    {
        return parser.parseClaimsJws(token).getBody();
    }

    private Map<String, PublicKey> fetchKeys()
    {
        try {
            ImmutableMap.Builder<String, PublicKey> builder = ImmutableMap.builder();
            for (JwksRsaKey rsaKey : jwksHttpClient.fetchKeys(jwksUri).keys()) {
                try {
                    builder.put(rsaKey.keyId(), rsaKey.toPublicKey());
                }
                catch (GeneralSecurityException e) {
                    throw new RuntimeException("Failed to convert JWKS key: " + rsaKey.keyId(), e);
                }
            }
            return builder.buildOrThrow();
        }
        catch (UnexpectedErrorException e) {
            throw new RuntimeException("Error fetching portal JWKS keys from " + jwksUri, e);
        }
    }

    private static final class KeyLocator
            implements Locator<Key>
    {
        private final AtomicReference<Map<String, PublicKey>> keys;

        KeyLocator(AtomicReference<Map<String, PublicKey>> keys)
        {
            this.keys = requireNonNull(keys, "keys is null");
        }

        @Override
        public Key locate(Header header)
        {
            String keyId = switch (header) {
                case JwsHeader jwsHeader -> jwsHeader.getKeyId();
                case JweHeader jweHeader -> jweHeader.getKeyId();
                default -> throw new UnsupportedJwtException("Cannot locate key for header: %s".formatted(header.getType()));
            };
            if (keyId == null) {
                throw new SecurityException("Key ID is required");
            }
            PublicKey key = keys.get().get(keyId);
            if (key == null) {
                throw new SecurityException("Unknown signing key ID: " + keyId);
            }
            return key;
        }
    }
}
