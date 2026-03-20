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
package io.trino.plugin.base.authtolocal.rule;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provider;
import io.airlift.log.Logger;
import io.trino.plugin.base.authtolocal.AuthToLocal;
import io.trino.plugin.base.authtolocal.AuthToLocalModule;
import io.trino.plugin.base.authtolocal.ForwardingAuthToLocal;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.security.ConnectorIdentity;

import java.nio.file.Path;
import java.util.Optional;

import static com.google.common.base.Suppliers.memoizeWithExpiration;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.base.util.JsonUtils.parseJson;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class RuleBasedAuthToLocalModule
        implements Module
{
    private final Optional<AuthToLocalModule.AuthToLocalBinding> authToLocalBinding;

    public RuleBasedAuthToLocalModule(Optional<AuthToLocalModule.AuthToLocalBinding> authToLocalBinding)
    {
        this.authToLocalBinding = requireNonNull(authToLocalBinding, "authToLocalBinding is null");
    }

    @Override
    public void configure(Binder binder)
    {
        if (authToLocalBinding.isEmpty()) {
            configBinder(binder).bindConfig(RuleBasedAuthToLocalConfig.class);
            binder.bind(AuthToLocal.class).toProvider(new AuthToLocalProvider(Key.get(RuleBasedAuthToLocalConfig.class)));
            return;
        }
        AuthToLocalModule.AuthToLocalBinding authToLocalBinding = this.authToLocalBinding.orElseThrow();
        configBinder(binder).bindConfig(RuleBasedAuthToLocalConfig.class, authToLocalBinding.getAnnotation(), authToLocalBinding.getPrefix());
        binder.bind(AuthToLocal.class)
                .annotatedWith(authToLocalBinding.getAnnotation())
                .toProvider(new AuthToLocalProvider(Key.get(RuleBasedAuthToLocalConfig.class, authToLocalBinding.getAnnotation())));
    }

    private static class AuthToLocalProvider
            implements Provider<AuthToLocal>
    {
        private static final Logger log = Logger.get(AuthToLocalProvider.class);

        private final Key<RuleBasedAuthToLocalConfig> keyForAuthToLocalConfig;
        private Injector injector;

        private AuthToLocalProvider(Key<RuleBasedAuthToLocalConfig> keyForAuthToLocalConfig)
        {
            this.keyForAuthToLocalConfig = requireNonNull(keyForAuthToLocalConfig, "keyForAuthToLocalConfig is null");
        }

        @Inject
        public void setInjector(Injector injector)
        {
            this.injector = injector;
        }

        @Override
        public AuthToLocal get()
        {
            RuleBasedAuthToLocalConfig config = injector.getInstance(keyForAuthToLocalConfig);
            Optional<String> configFile = config.getConfigFile();
            if (configFile.isEmpty()) {
                return ConnectorIdentity::getUser;
            }

            if (config.getRefreshPeriod() != null) {
                CatalogName catalogName = injector.getInstance(CatalogName.class);
                return ForwardingAuthToLocal.of(memoizeWithExpiration(
                        () -> {
                            log.info("Refreshing auth to local for %s from %s", catalogName, configFile.get());
                            return createAuthToLocal(configFile.get());
                        },
                        config.getRefreshPeriod().toMillis(),
                        MILLISECONDS));
            }
            return createAuthToLocal(configFile.get());
        }
    }

    @VisibleForTesting
    static RuleBasedAuthToLocal createAuthToLocal(String configFile)
    {
        return parseJson(Path.of(configFile), RuleBasedAuthToLocal.class);
    }
}
