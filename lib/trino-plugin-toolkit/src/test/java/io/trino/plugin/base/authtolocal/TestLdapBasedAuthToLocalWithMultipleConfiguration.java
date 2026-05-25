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
package io.trino.plugin.base.authtolocal;

import com.google.common.collect.ImmutableMap;
import com.google.inject.BindingAnnotation;
import com.google.inject.CreationException;
import com.google.inject.Injector;
import com.google.inject.Key;
import io.airlift.bootstrap.Bootstrap;
import io.trino.plugin.base.authtolocal.ldap.TestingLdapServerForAuthToLocal;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.io.Resources.getResource;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestLdapBasedAuthToLocalWithMultipleConfiguration
{
    private TestingLdapServerForAuthToLocal ldapServerForAuthToLocal;

    @BeforeAll
    public void setup()
            throws Exception
    {
        ldapServerForAuthToLocal = new TestingLdapServerForAuthToLocal();
    }

    @AfterAll
    public void close()
            throws Exception
    {
        try (var _ = ldapServerForAuthToLocal) {
            ldapServerForAuthToLocal = null;
        }
    }

    @ParameterizedTest
    @MethodSource("configurationDataProvider")
    @Execution(SAME_THREAD) // this Parameterized Test creates user "charlie" for each parameter set. Username cannot be randomized as it's hardcoded in one of parameters
    public void testAuthToLocalWithMultipleConfiguration(Key<AuthToLocal> key, String expectedValue)
            throws Exception
    {
        try (var _ = ldapServerForAuthToLocal.createUserWithGivenName("charlie", "mapped-charlie")) {
            Injector injector = new Bootstrap(
                    new AuthToLocalModule(),
                    new AuthToLocalModule("annotation1", ForAnnotation1.class),
                    new AuthToLocalModule("annotation2", ForAnnotation2.class),
                    new AuthToLocalModule("annotation3", ForAnnotation3.class))
                    .setRequiredConfigurationProperties(ImmutableMap.<String, String>builder()
                            .putAll(ldapServerForAuthToLocal.getAuthToLocalConfigurationWithAttribute("givenName"))
                            .putAll(ldapServerForAuthToLocal.getAuthToLocalConfigurationWithAttribute("givenName").entrySet().stream()
                                    .collect(toImmutableMap(entry -> "annotation1." + entry.getKey(), Map.Entry::getValue)))
                            .putAll(ldapServerForAuthToLocal.getAuthToLocalConfigurationWithAttribute("cn").entrySet().stream()
                                    .collect(toImmutableMap(entry -> "annotation2." + entry.getKey(), Map.Entry::getValue)))
                            .put("annotation3.auth-to-local.config-file", getResource("TestLdapBasedAuthToLocalWithMultipleConfiguration-auth-to-local.json").getPath())
                            .buildOrThrow())
                    .initialize();

            AuthToLocal authToLocalForAnnotation = injector.getInstance(key);
            assertThat(authToLocalForAnnotation.translate(ConnectorIdentity.forUser("charlie").build())).isEqualTo(expectedValue);
        }
    }

    public static Object[][] configurationDataProvider()
    {
        return new Object[][] {
                {Key.get(AuthToLocal.class), "mapped-charlie"},
                {Key.get(AuthToLocal.class, ForAnnotation1.class), "mapped-charlie"},
                {Key.get(AuthToLocal.class, ForAnnotation2.class), "charlie"},
                {Key.get(AuthToLocal.class, ForAnnotation3.class), "from-auth-to-local"},
        };
    }

    @Test
    public void testAuthToLocalWithDifferentPrefixAndSameAnnotation()
    {
        assertThatThrownBy(() -> new Bootstrap(
                new AuthToLocalModule("annotation1", ForAnnotation1.class),
                new AuthToLocalModule("annotation2", ForAnnotation1.class))
                .setRequiredConfigurationProperties(ImmutableMap.<String, String>builder()
                        .putAll(ldapServerForAuthToLocal.getAuthToLocalConfigurationWithAttribute("givenName").entrySet().stream()
                                .collect(toImmutableMap(entry -> "annotation1." + entry.getKey(), Map.Entry::getValue)))
                        .buildOrThrow())
                .initialize())
                .isInstanceOf(CreationException.class)
                .hasMessageContaining("AuthToLocal annotated with interface TestLdapBasedAuthToLocalWithMultipleConfiguration$ForAnnotation1 was bound multiple times");
    }

    @Test
    public void testAuthToLocalWithSamePrefixAndDifferentAnnotation()
    {
        assertThatThrownBy(() -> new Bootstrap(
                new AuthToLocalModule("annotation1", ForAnnotation1.class),
                new AuthToLocalModule("annotation1", ForAnnotation2.class))
                .setRequiredConfigurationProperties(ImmutableMap.<String, String>builder()
                        .putAll(ldapServerForAuthToLocal.getAuthToLocalConfigurationWithAttribute("givenName").entrySet().stream()
                                .collect(toImmutableMap(entry -> "annotation1." + entry.getKey(), Map.Entry::getValue)))
                        .buildOrThrow())
                .initialize())
                .isInstanceOf(CreationException.class)
                .hasMessageContaining("AuthToLocal annotated with @Named(\"annotation1\") was bound multiple times.");
    }

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface ForAnnotation1 {}

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface ForAnnotation2 {}

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface ForAnnotation3 {}
}
