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

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.name.Names;
import io.trino.plugin.base.authtolocal.AuthToLocal;
import io.trino.plugin.base.authtolocal.AuthToLocalModule.AuthToLocalBinding;

import java.util.Optional;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class CachingAuthToLocalModule
        implements Module
{
    private final Optional<AuthToLocalBinding> authToLocalBinding;

    public CachingAuthToLocalModule(Optional<AuthToLocalBinding> authToLocalBinding)
    {
        this.authToLocalBinding = requireNonNull(authToLocalBinding, "authToLocalBinding is null");
    }

    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(CachingAuthToLocalConfig.class);
        Provider<CachingAuthToLocalConfig> configProvider = binder.getProvider(Key.get(CachingAuthToLocalConfig.class));
        if (authToLocalBinding.isPresent()) {
            AuthToLocalBinding authToLocalBinding = this.authToLocalBinding.get();
            Provider<AuthToLocal> authToLocalProvider = binder.getProvider(Key.get(AuthToLocal.class, Names.named(authToLocalBinding.getPrefix())));
            binder.bind(AuthToLocal.class)
                    .annotatedWith(authToLocalBinding.getAnnotation())
                    .toProvider(() -> new CachingAuthToLocal(authToLocalProvider.get(), configProvider.get()))
                    .in(SINGLETON);
            return;
        }
        Provider<AuthToLocal> authToLocalProvider = binder.getProvider(Key.get(AuthToLocal.class, Names.named("")));
        binder.bind(AuthToLocal.class).toProvider(() -> new CachingAuthToLocal(authToLocalProvider.get(), configProvider.get())).in(SINGLETON);
    }
}
