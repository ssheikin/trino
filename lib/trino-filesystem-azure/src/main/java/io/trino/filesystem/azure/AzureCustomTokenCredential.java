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
package io.trino.filesystem.azure;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import io.trino.spi.TrinoException;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;

import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static java.util.Objects.requireNonNull;

public class AzureCustomTokenCredential
        implements TokenCredential
{
    private final String token;

    public AzureCustomTokenCredential(String token)
    {
        if (token.isEmpty()) {
            throw new TrinoException(GENERIC_USER_ERROR, "Unable to find Azure AD authentication token");
        }
        this.token = requireNonNull(token, "token is null");
    }

    @Override
    public Mono<AccessToken> getToken(TokenRequestContext tokenRequestContext)
    {
        return Mono.just(new AccessToken(token, OffsetDateTime.now()));
    }
}
