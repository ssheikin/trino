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

import io.airlift.units.Duration;
import io.trino.plugin.base.authtolocal.AuthToLocal;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.Test;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCachingAuthToLocal
{
    @Test
    public void testTranslateWithCaching()
    {
        MockAuthToLocal authToLocal = new MockAuthToLocal();
        CachingAuthToLocal cachingAuthToLocal = new CachingAuthToLocal(
                authToLocal,
                new CachingAuthToLocalConfig().setCacheTtl(new Duration(1, DAYS)));
        cachingAuthToLocal.translate(ConnectorIdentity.forUser("alice").build());
        for (int i = 0; i < 5; i++) {
            cachingAuthToLocal.translate(ConnectorIdentity.forUser("alice").build());
            assertThat(authToLocal.getInvocationCount()).isEqualTo(1);
        }
        // For a different user
        cachingAuthToLocal.translate(ConnectorIdentity.forUser("bob").build());
        assertThat(authToLocal.getInvocationCount()).isEqualTo(2);
    }

    @Test
    public void testTranslateWithZeroCacheTtl()
    {
        MockAuthToLocal authToLocal = new MockAuthToLocal();
        CachingAuthToLocal cachingAuthToLocal = new CachingAuthToLocal(
                authToLocal,
                new CachingAuthToLocalConfig()
                        .setCacheTtl(new Duration(0, SECONDS)));
        for (int i = 0; i < 5; i++) {
            cachingAuthToLocal.translate(ConnectorIdentity.forUser("alice").build());
            assertThat(authToLocal.getInvocationCount()).isEqualTo(i + 1);
        }
        // For a different user
        cachingAuthToLocal.translate(ConnectorIdentity.forUser("bob").build());
        assertThat(authToLocal.getInvocationCount()).isEqualTo(6);
    }

    @Test
    public void testTranslateWithZeroCacheSize()
    {
        MockAuthToLocal authToLocal = new MockAuthToLocal();
        CachingAuthToLocal cachingAuthToLocal = new CachingAuthToLocal(
                authToLocal,
                new CachingAuthToLocalConfig()
                        .setCacheTtl(new Duration(1, DAYS))
                        .setCacheMaximumSize(0));
        for (int i = 0; i < 5; i++) {
            cachingAuthToLocal.translate(ConnectorIdentity.forUser("alice").build());
            assertThat(authToLocal.getInvocationCount()).isEqualTo(i + 1);
        }
        // For a different user
        cachingAuthToLocal.translate(ConnectorIdentity.forUser("bob").build());
        assertThat(authToLocal.getInvocationCount()).isEqualTo(6);
    }

    public static class MockAuthToLocal
            implements AuthToLocal
    {
        private int invocationCount;

        @Override
        public String translate(ConnectorIdentity identity)
        {
            invocationCount++;
            return "identity";
        }

        public int getInvocationCount()
        {
            return invocationCount;
        }
    }
}
