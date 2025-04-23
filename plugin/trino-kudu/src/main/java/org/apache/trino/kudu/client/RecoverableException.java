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
import org.apache.yetus.audience.InterfaceStability;

/**
 * An exception that's possible to retry.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
@SuppressWarnings("serial")
class RecoverableException
        extends KuduException
{
    /**
     * Constructor.
     *
     * @param status status object containing the reason for the exception
     * trace.
     */
    RecoverableException(Status status)
    {
        super(status);
    }

    /**
     * Constructor.
     *
     * @param status status object containing the reason for the exception
     * @param cause The exception that caused this one to be thrown.
     */
    RecoverableException(Status status, Throwable cause)
    {
        super(status, cause);
    }
}
