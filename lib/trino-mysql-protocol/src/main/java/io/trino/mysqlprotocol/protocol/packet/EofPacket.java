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
package io.trino.mysqlprotocol.protocol.packet;

import io.netty.buffer.ByteBuf;
import io.trino.mysqlprotocol.protocol.MysqlPacketBuffer;

/**
 * Legacy MySQL Protocol::EOF_Packet (header 0xFE, 5 bytes total) used when CLIENT_DEPRECATE_EOF is NOT negotiated.
 */
public final class EofPacket
        implements MysqlPacket
{
    private final int warnings;
    private final int statusFlags;

    public EofPacket(int warnings, int statusFlags)
    {
        this.warnings = warnings;
        this.statusFlags = statusFlags;
    }

    @Override
    public void writeTo(ByteBuf out)
    {
        MysqlPacketBuffer.writeInt1(out, 0xFE);
        MysqlPacketBuffer.writeInt2(out, warnings);
        MysqlPacketBuffer.writeInt2(out, statusFlags);
    }
}
