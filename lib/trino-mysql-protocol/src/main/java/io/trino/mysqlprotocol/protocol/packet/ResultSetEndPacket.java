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
 * Resultset-terminating OK packet (0xFE header with payload &gt; 5 bytes) required when CLIENT_DEPRECATE_EOF is negotiated.
 */
public final class ResultSetEndPacket
        implements MysqlPacket
{
    private final int statusFlags;
    private final int warnings;

    public ResultSetEndPacket(int statusFlags, int warnings)
    {
        this.statusFlags = statusFlags;
        this.warnings = warnings;
    }

    @Override
    public void writeTo(ByteBuf out)
    {
        MysqlPacketBuffer.writeInt1(out, 0xFE);
        MysqlPacketBuffer.writeLenEncInt(out, 0); // affected_rows
        MysqlPacketBuffer.writeLenEncInt(out, 0); // last_insert_id
        MysqlPacketBuffer.writeInt2(out, statusFlags);
        MysqlPacketBuffer.writeInt2(out, warnings);
        MysqlPacketBuffer.writeLenEncInt(out, 0); // empty info string (ensures payload > 5 bytes)
    }
}
