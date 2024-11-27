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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.plugin.base.authtolocal.AuthToLocal;
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;

import java.security.Principal;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RuleBasedAuthToLocal
        implements AuthToLocal
{
    private static final Logger LOG = Logger.get(RuleBasedAuthToLocal.class);

    private final List<AuthToLocalRule> rules;

    @JsonCreator
    public RuleBasedAuthToLocal(
            @JsonProperty("rules") List<AuthToLocalRule> rules)
    {
        this.rules = ImmutableList.copyOf(requireNonNull(rules, "rules is null"));
    }

    @Override
    public String translate(ConnectorIdentity identity)
    {
        for (AuthToLocalRule rule : rules) {
            Optional<String> translation = rule.apply(identity);
            if (translation.isPresent()) {
                LOG.debug("%s was translated to %s", identity, translation.get());
                return translation.get();
            }
        }
        throw new TrinoException(CONFIGURATION_INVALID, format(
                "No auth-to-local rule was found for user [%s] and principal %s",
                identity.getUser(),
                identity.getPrincipal()
                        .map(Principal::getName)
                        .map(value -> format("[%s]", value))
                        .orElse("(not set)")));
    }
}
