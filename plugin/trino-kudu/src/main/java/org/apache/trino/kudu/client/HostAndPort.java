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

package org.apache.trino.kudu.client;

import org.apache.yetus.audience.InterfaceAudience;

import java.net.InetSocketAddress;
import java.util.Objects;

/**
 * A simple wrapper around InetSocketAddress to prevent
 * accidentally introducing DNS lookups.
 * p
 * The HostAndPort implementation in Guava is not used
 * because Guava is shaded and relocated in Kudu preventing
 * it from being used as a parameter or return value on
 * public methods. Additionally Guava's HostAndPort
 * implementation is marked as beta.
 */
@InterfaceAudience.Private
public class HostAndPort
{
    private final InetSocketAddress address;

    public HostAndPort(String host, int port)
    {
        // Using createUnresolved ensures no lookups will occur.
        this.address = InetSocketAddress.createUnresolved(host, port);
    }

    public String getHost()
    {
        // Use getHostString to ensure no reverse lookup is done.
        return address.getHostString();
    }

    public int getPort()
    {
        return address.getPort();
    }

    public InetSocketAddress getAddress()
    {
        return address;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HostAndPort that)) {
            return false;
        }
        return Objects.equals(address, that.address);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(address);
    }

    @Override
    public String toString()
    {
        return address.getHostName() + ":" + address.getPort();
    }
}
