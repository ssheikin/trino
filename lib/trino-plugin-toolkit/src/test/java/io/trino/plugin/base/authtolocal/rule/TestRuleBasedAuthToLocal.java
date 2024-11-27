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

import com.google.common.io.Resources;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URISyntaxException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestRuleBasedAuthToLocal
{
    @Test
    public void testConstantPattern()
    {
        RuleBasedAuthToLocal authToLocal = RuleBasedAuthToLocalModule.createAuthToLocal(getResourceFile("TestRuleBasedAuthToLocal-constant-pattern.json"));
        assertThatThrownBy(() -> authToLocal.translate(ConnectorIdentity.ofUser("alice")))
                .hasMessage("No auth-to-local rule was found for user [alice] and principal (not set)");
        assertThat(authToLocal.translate(ConnectorIdentity.ofUser("bob"))).isEqualTo("this_is_replaced");
    }

    @Test
    public void testReplacement()
    {
        RuleBasedAuthToLocal authToLocal = RuleBasedAuthToLocalModule.createAuthToLocal(getResourceFile("TestRuleBasedAuthToLocal-replacement.json"));
        assertThatThrownBy(() -> authToLocal.translate(ConnectorIdentity.ofUser("alice")))
                .hasMessage("No auth-to-local rule was found for user [alice] and principal (not set)");
        assertThat(authToLocal.translate(ConnectorIdentity.ofUser("alice@default-substitution"))).isEqualTo("alice@default-substitution");
        assertThat(authToLocal.translate(ConnectorIdentity.ofUser("alice@explicit-substitution0"))).isEqualTo("alice@explicit-substitution0");
        assertThat(authToLocal.translate(ConnectorIdentity.ofUser("alice@explicit-substitution1"))).isEqualTo("alice");
    }

    @Test
    public void testReplaceAll()
    {
        RuleBasedAuthToLocal authToLocal = RuleBasedAuthToLocalModule.createAuthToLocal(getResourceFile("TestRuleBasedAuthToLocal-replace-all.json"));
        assertThat(authToLocal.translate(ConnectorIdentity.ofUser("alice"))).isEqualTo("some-user-substituted");
        assertThat(authToLocal.translate(ConnectorIdentity.ofUser("bob"))).isEqualTo("some-user-substituted");
    }

    private String getResourceFile(String resourceName)
    {
        try {
            return new File(Resources.getResource(getClass(), resourceName).toURI()).getPath();
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
