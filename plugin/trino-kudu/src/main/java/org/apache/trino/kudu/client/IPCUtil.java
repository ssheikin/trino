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

import org.apache.kudu.shaded.com.google.protobuf.CodedOutputStream;
import org.apache.kudu.shaded.com.google.protobuf.Message;
import org.apache.yetus.audience.InterfaceAudience;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Helper methods for RPCs.
 */
@InterfaceAudience.Private
public class IPCUtil
{
    private IPCUtil()
    {
    }

    public static int write(final OutputStream dos, final Message header, final Message param)

            throws IOException
    {
        // Must calculate total size and write that first so other side can read it all in in one
        // swoop.  This is dictated by how the server is currently written.  Server needs to change
        // if we are to be able to write without the length prefixing.
        int totalSize = IPCUtil.getTotalSizeWhenWrittenDelimited(header, param);
        return write(dos, header, param, totalSize);
    }

    private static int write(final OutputStream dos, final Message header, final Message param,
                             final int totalSize)

            throws IOException
    {
        // I confirmed toBytes does same as say DataOutputStream#writeInt.
        dos.write(toBytes(totalSize));
        header.writeDelimitedTo(dos);
        if (param != null) {
            param.writeDelimitedTo(dos);
        }
        dos.flush();
        return totalSize;
    }

    /**
     * @return Size on the wire when the two messages are written with writeDelimitedTo
     */
    public static int getTotalSizeWhenWrittenDelimited(Message... messages)
    {
        int totalSize = 0;
        for (Message m : messages) {
            if (m == null) {
                continue;
            }
            totalSize += m.getSerializedSize();
            totalSize += CodedOutputStream.computeUInt32SizeNoTag(m.getSerializedSize());
        }
        return totalSize;
    }

    public static byte[] toBytes(int val)
    {
        byte[] b = new byte[4];
        for (int i = 3; i > 0; i--) {
            b[i] = (byte) val;
            val >>>= 8;
        }
        b[0] = (byte) val;
        return b;
    }
}
