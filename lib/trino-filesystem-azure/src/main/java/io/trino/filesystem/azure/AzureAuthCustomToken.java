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

import com.azure.core.credential.TokenCredential;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import io.trino.filesystem.azure.AzureFileSystemConfig.AuthType;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public final class AzureAuthCustomToken
        implements AzureAuth
{
    private final TokenCredential credential;

    public AzureAuthCustomToken(TokenCredential credential, AuthType authType)
    {
        checkState(authType.equals(AuthType.DEFAULT),
                "When using Azure AD pass-through, no other authentication mechanisms should be set. Found: %s",
                authType);
        this.credential = requireNonNull(credential, "credential is null");
    }

    @Override
    public void setAuth(String storageAccount, BlobContainerClientBuilder builder)
    {
        builder.credential(credential);
    }

    @Override
    public void setAuth(String storageAccount, DataLakeServiceClientBuilder builder)
    {
        builder.credential(credential);
    }
}
