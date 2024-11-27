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
package io.trino.plugin.base.authtolocal.cache;

import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.plugin.base.authtolocal.AuthToLocal;
import io.trino.spi.TrinoException;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.ConnectorIdentity;

import java.security.Principal;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class CachingAuthToLocal
        implements AuthToLocal
{
    private final LoadingCache<ConnectorIdentityKey, String> cache;

    public CachingAuthToLocal(AuthToLocal delegate, CachingAuthToLocalConfig config)
    {
        requireNonNull(delegate, "delegate is null");
        requireNonNull(config, "config is null");
        cache = EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(config.getCacheTtl().toMillis(), MILLISECONDS)
                .maximumSize(config.getCacheMaximumSize())
                .shareNothingWhenDisabled()
                .build(CacheLoader.from(key -> delegate.translate(key.toConnectorIdentity())));
    }

    @Override
    public String translate(ConnectorIdentity identity)
    {
        try {
            return cache.getUnchecked(new ConnectorIdentityKey(identity));
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw e;
        }
    }

    // TODO use IdentityCacheMapping (https://starburstdata.atlassian.net/browse/SEP-9321)
    private static class ConnectorIdentityKey
    {
        private final String user;
        private final Optional<String> principal;

        public ConnectorIdentityKey(ConnectorIdentity identity)
        {
            requireNonNull(identity, "identity is null");
            user = identity.getUser();
            principal = identity.getPrincipal()
                    .map(Principal::getName);
        }

        public ConnectorIdentity toConnectorIdentity()
        {
            return ConnectorIdentity.forUser(user)
                    .withPrincipal(principal.map(BasicPrincipal::new))
                    .build();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ConnectorIdentityKey that = (ConnectorIdentityKey) o;
            return Objects.equals(user, that.user) && Objects.equals(principal, that.principal);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(user, principal);
        }
    }
}
