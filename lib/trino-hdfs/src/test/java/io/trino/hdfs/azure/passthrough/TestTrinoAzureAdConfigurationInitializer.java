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
package io.trino.hdfs.azure.passthrough;

import io.trino.hdfs.azure.HiveAzureConfig;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTrinoAzureAdConfigurationInitializer
{
    @ParameterizedTest
    @MethodSource("configWithAbfsAuth")
    public void testThrowsWhenMultipleOAuthMethods(HiveAzureConfig configWithAbfsAuth)
    {
        assertThatThrownBy(() -> new TrinoAzureAdConfigurationInitializer(configWithAbfsAuth))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("When using Azure AD pass-through, no other ABFS credentials should be set");
    }

    public static Object[][] configWithAbfsAuth()
    {
        return new Object[][] {
                {new HiveAzureConfig().setAbfsAccessKey("ak").setAbfsStorageAccount("acc")},
                {new HiveAzureConfig().setAbfsOAuthClientId("id").setAbfsOAuthClientSecret("secret").setAbfsOAuthClientEndpoint("endpoint")}
        };
    }
}
