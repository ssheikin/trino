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

package io.trino.server.resultscache;

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.SystemSessionPropertiesProvider;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class ResultsCacheModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(conditionalModule(
                CachingConfig.class,
                CachingConfig::isResultsCacheEnabled,
                new EnabledResultsCacheModule(),
                new DisabledResultsCacheModule()));
    }

    private static class EnabledResultsCacheModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            newSetBinder(binder, SystemSessionPropertiesProvider.class).addBinding().to(ResultsCacheSessionProperties.class);
            configBinder(binder).bindConfig(ResultsCacheConfig.class);
            newOptionalBinder(binder, CacheClient.class).setDefault().toInstance(cacheEntry -> {
                throw new UnsupportedOperationException();
            });
            binder.bind(ResultsCacheManager.class).to(ActiveResultsCacheManager.class).in(SINGLETON);
        }
    }

    private static class DisabledResultsCacheModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            binder.bind(ResultsCacheManager.class).toInstance(new ResultsCacheManager() {});
        }
    }
}
