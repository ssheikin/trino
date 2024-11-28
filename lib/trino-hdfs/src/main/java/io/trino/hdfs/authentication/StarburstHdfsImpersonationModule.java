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
package io.trino.hdfs.authentication;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Provider;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.authtolocal.AuthToLocal;
import io.trino.plugin.base.authtolocal.AuthToLocalModule;
import io.trino.plugin.base.security.UserNameProvider;
import io.trino.spi.security.ConnectorIdentity;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static java.util.Objects.requireNonNull;

public class StarburstHdfsImpersonationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(new AuthToLocalModule("hive.hdfs", ForHdfs.class));
        newOptionalBinder(binder, Key.get(UserNameProvider.class, ForHdfs.class))
                .setBinding()
                .toProvider(AuthToLocalUserNameProviderFactory.class)
                .in(SINGLETON);
    }

    private static class AuthToLocalUserNameProviderFactory
            implements Provider<AuthToLocalUserNameProvider>
    {
        private final AuthToLocal authToLocal;

        @Inject
        public AuthToLocalUserNameProviderFactory(@ForHdfs AuthToLocal authToLocal)
        {
            this.authToLocal = requireNonNull(authToLocal, "authLocal is null");
        }

        @Override
        public AuthToLocalUserNameProvider get()
        {
            return new AuthToLocalUserNameProvider(authToLocal);
        }
    }

    private static class AuthToLocalUserNameProvider
            implements UserNameProvider
    {
        private final AuthToLocal authToLocal;

        public AuthToLocalUserNameProvider(AuthToLocal authToLocal)
        {
            this.authToLocal = requireNonNull(authToLocal, "authToLocal is null");
        }

        @Override
        public String get(ConnectorIdentity identity)
        {
            return authToLocal.translate(identity);
        }
    }
}
