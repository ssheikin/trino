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
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;

import java.security.Principal;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static java.util.Objects.requireNonNull;

public class AuthToLocalRule
{
    enum Case
    {
        KEEP(Function.identity()),
        LOWER(value -> value.toLowerCase(Locale.ENGLISH)),
        UPPER(value -> value.toUpperCase(Locale.ENGLISH)),
        /**/;

        private final Function<String, String> transformation;

        Case(Function<String, String> transformation)
        {
            this.transformation = requireNonNull(transformation, "transformation is null");
        }

        public final String transform(String value)
        {
            return transformation.apply(value);
        }
    }

    enum Match
    {
        USER(ConnectorIdentity::getUser),
        PRINCIPAL(identity -> identity.getPrincipal()
                .map(Principal::getName)
                .orElseThrow(() -> new TrinoException(CONFIGURATION_INVALID, "Principal is not present"))),
        /**/;

        private final Function<ConnectorIdentity, String> getValueFunction;

        Match(Function<ConnectorIdentity, String> getValueFunction)
        {
            this.getValueFunction = requireNonNull(getValueFunction, "getValueFunction is null");
        }

        public String getValue(ConnectorIdentity identity)
        {
            return getValueFunction.apply(identity);
        }
    }

    private final Match match;
    private final Pattern pattern;
    private final String substitution;
    private final Case targetCase;

    @JsonCreator
    public AuthToLocalRule(
            @JsonProperty("match") Optional<Match> match,
            @JsonProperty("case") Optional<Case> targetCase,
            @JsonProperty("pattern") Pattern pattern,
            @JsonProperty("substitution") Optional<String> substitution)
    {
        this.match = requireNonNull(match, "match is null").orElse(Match.USER);
        this.targetCase = requireNonNull(targetCase, "targetCase is null").orElse(Case.KEEP);
        this.pattern = requireNonNull(pattern, "pattern is null");
        this.substitution = requireNonNull(substitution, "substitution is null").orElse("$0");
    }

    public Optional<String> apply(ConnectorIdentity identity)
    {
        String value = match.getValue(identity);
        Matcher matcher = pattern.matcher(value);
        if (!matcher.matches()) {
            return Optional.empty();
        }
        StringBuilder sb = new StringBuilder();
        matcher.appendReplacement(sb, substitution);
        matcher.appendTail(sb);
        return Optional.of(targetCase.transform(sb.toString()));
    }
}
