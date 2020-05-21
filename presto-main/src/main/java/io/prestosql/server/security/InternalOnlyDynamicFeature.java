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
package io.prestosql.server.security;

import com.google.common.collect.ImmutableSet;
import io.airlift.discovery.server.DynamicAnnouncementResource;
import io.airlift.discovery.store.StoreResource;
import io.prestosql.server.TaskResource;

import javax.annotation.Priority;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.DynamicFeature;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.FeatureContext;

import java.util.Set;

public class InternalOnlyDynamicFeature
        implements DynamicFeature
{
    private static final Set<Class<?>> INTERNAL_ONLY_RESOURCES = ImmutableSet.<Class<?>>builder()
            .add(TaskResource.class)
            .add(DynamicAnnouncementResource.class)
            .add(StoreResource.class)
            .build();

    @Override
    public void configure(ResourceInfo resourceInfo, FeatureContext context)
    {
        if (INTERNAL_ONLY_RESOURCES.contains(resourceInfo.getResourceClass())) {
            context.register(new InternalOnlyRequestFilter());
        }
    }

    @Priority(Priorities.AUTHENTICATION)
    private static class InternalOnlyRequestFilter
            implements ContainerRequestFilter
    {
        @Override
        public void filter(ContainerRequestContext request)
        {
            if (!(request.getSecurityContext().getUserPrincipal() instanceof InternalPrincipal)) {
                throw new ForbiddenException("Internal only resource");
            }
        }
    }
}
