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
import io.trino.mysqlprotocol.protocol.MysqlCapability;
import io.trino.mysqlprotocol.protocol.MysqlPacketBuffer;

import static java.util.Objects.requireNonNull;

/**
 * MySQL Protocol::OK_Packet (header 0x00).
 */
public final class OkPacket
        implements MysqlPacket
{
    private final long affectedRows;
    private final long lastInsertId;
    private final int statusFlags;
    private final int warnings;
    private final String info;
    private final int capabilities;

    public OkPacket(long affectedRows, long lastInsertId, int statusFlags, int warnings, String info, int capabilities)
    {
        this.affectedRows = affectedRows;
        this.lastInsertId = lastInsertId;
        this.statusFlags = statusFlags;
        this.warnings = warnings;
        this.info = requireNonNull(info, "info is null");
        this.capabilities = capabilities;
    }

    @Override
    public void writeTo(ByteBuf out)
    {
        MysqlCapability cap = new MysqlCapability(capabilities);
        MysqlPacketBuffer.writeInt1(out, 0x00);
        MysqlPacketBuffer.writeLenEncInt(out, affectedRows);
        MysqlPacketBuffer.writeLenEncInt(out, lastInsertId);
        if (cap.isProtocol41()) {
            MysqlPacketBuffer.writeInt2(out, statusFlags);
            MysqlPacketBuffer.writeInt2(out, warnings);
        }
        else if (cap.has(MysqlCapability.CLIENT_TRANSACTIONS)) {
            MysqlPacketBuffer.writeInt2(out, statusFlags);
        }
        if (cap.isSessionTrack()) {
            MysqlPacketBuffer.writeLenEncString(out, info);
        }
        else if (!info.isEmpty()) {
            MysqlPacketBuffer.writeLenEncString(out, info);
        }
        else if (cap.isDeprecatedEof()) {
            // Connector/J expects the info length-encoded field when CLIENT_DEPRECATE_EOF is active
            MysqlPacketBuffer.writeLenEncInt(out, 0);
        }
    }
}
