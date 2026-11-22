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

import static com.google.common.base.Preconditions.checkArgument;

/**
 * MySQL Protocol::SSLRequest packet (32 bytes) sent before TLS upgrade.
 */
public record SslRequestPacket(int capabilities, int maxPacketSize, int characterSet)
{
    public static SslRequestPacket readFrom(ByteBuf in)
    {
        checkArgument(in.readableBytes() >= 32, "Truncated SSLRequest packet: %s bytes", in.readableBytes());
        int capabilities = MysqlPacketBuffer.readInt4(in);
        int maxPacketSize = MysqlPacketBuffer.readInt4(in);
        int characterSet = MysqlPacketBuffer.readInt1(in);
        in.skipBytes(23);
        return new SslRequestPacket(capabilities, maxPacketSize, characterSet);
    }
}
