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
import io.trino.mysqlprotocol.protocol.MysqlFieldType;
import io.trino.mysqlprotocol.protocol.MysqlPacketBuffer;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * MySQL Protocol::HandshakeV10 initial server greeting packet.
 */
public final class HandshakeV10Packet
        implements MysqlPacket
{
    public static final int PROTOCOL_VERSION = 10;
    public static final int SCRAMBLE_LENGTH = 20;
    public static final String MYSQL_NATIVE_PASSWORD = "mysql_native_password";
    public static final String MYSQL_CLEAR_PASSWORD = "mysql_clear_password";

    private final String serverVersion;
    private final int connectionId;
    private final byte[] authPluginData;
    private final int capabilityFlags;
    private final int statusFlags;
    private final String authPluginName;

    public HandshakeV10Packet(
            String serverVersion,
            int connectionId,
            byte[] authPluginData,
            int capabilityFlags,
            int statusFlags,
            String authPluginName)
    {
        this.serverVersion = requireNonNull(serverVersion, "serverVersion is null");
        this.connectionId = connectionId;
        this.authPluginData = requireNonNull(authPluginData, "authPluginData is null").clone();
        checkArgument(this.authPluginData.length == SCRAMBLE_LENGTH, "authPluginData must be %s bytes", SCRAMBLE_LENGTH);
        this.capabilityFlags = capabilityFlags;
        this.statusFlags = statusFlags;
        this.authPluginName = requireNonNull(authPluginName, "authPluginName is null");
    }

    public byte[] authPluginData()
    {
        return authPluginData.clone();
    }

    public int capabilityFlags()
    {
        return capabilityFlags;
    }

    public String authPluginName()
    {
        return authPluginName;
    }

    @Override
    public void writeTo(ByteBuf out)
    {
        MysqlCapability capability = new MysqlCapability(capabilityFlags);
        MysqlPacketBuffer.writeInt1(out, PROTOCOL_VERSION);
        MysqlPacketBuffer.writeNulTerminatedString(out, serverVersion);
        MysqlPacketBuffer.writeInt4(out, connectionId);
        // First 8 bytes of auth plugin data
        out.writeBytes(authPluginData, 0, 8);
        // filler
        MysqlPacketBuffer.writeInt1(out, 0);
        // lower 2 bytes of capability flags
        MysqlPacketBuffer.writeInt2(out, capabilityFlags & 0xFFFF);
        // character set (utf8mb4)
        MysqlPacketBuffer.writeInt1(out, MysqlFieldType.CHARSET_UTF8MB4);
        // status flags
        MysqlPacketBuffer.writeInt2(out, statusFlags);
        // upper 2 bytes of capability flags
        MysqlPacketBuffer.writeInt2(out, (capabilityFlags >>> 16) & 0xFFFF);
        if (capability.isPluginAuth()) {
            MysqlPacketBuffer.writeInt1(out, authPluginData.length + 1);
        }
        else {
            MysqlPacketBuffer.writeInt1(out, 0);
        }
        // reserved 10 zero bytes
        out.writeZero(10);
        if (capability.isSecureConnection()) {
            out.writeBytes(authPluginData, 8, 12);
            MysqlPacketBuffer.writeInt1(out, 0);
        }
        if (capability.isPluginAuth()) {
            MysqlPacketBuffer.writeNulTerminatedString(out, authPluginName);
        }
    }
}
