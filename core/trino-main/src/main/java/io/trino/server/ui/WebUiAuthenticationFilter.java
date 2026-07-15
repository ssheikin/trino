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
package io.trino.server.ui;

import jakarta.annotation.Priority;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;

@Priority(Priorities.AUTHENTICATION)
public interface WebUiAuthenticationFilter
        extends ContainerRequestFilter
{
    /**
     * Whether the request carries this filter's UI credential material (e.g. a session cookie),
     * regardless of whether that material is still valid. Lets a caller distinguish "no credential
     * present, forward anonymously" from "credential present but invalid, issue a challenge".
     * Defaults to {@code false} so an unhandled filter type is treated as carrying no credential.
     */
    default boolean hasCredential(ContainerRequestContext request)
    {
        return false;
    }
}
