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

import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import io.trino.mysqlprotocol.protocol.MysqlCapability;
import io.trino.mysqlprotocol.protocol.MysqlPacketBuffer;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * MySQL Protocol::HandshakeResponse41 packet sent by clients during login.
 */
public record HandshakeResponse41Packet(
        int capabilities,
        int maxPacketSize,
        int characterSet,
        String username,
        byte[] authResponse,
        Optional<String> database,
        Optional<String> authPluginName,
        Map<String, String> connectAttributes)
{
    public HandshakeResponse41Packet
    {
        authResponse = authResponse.clone();
        connectAttributes = ImmutableMap.copyOf(connectAttributes);
    }

    @Override
    public byte[] authResponse()
    {
        return authResponse.clone();
    }

    public static HandshakeResponse41Packet readFrom(ByteBuf in)
    {
        int capabilities = MysqlPacketBuffer.readInt4(in);
        MysqlCapability cap = new MysqlCapability(capabilities);
        checkArgument(cap.isProtocol41(), "Client does not support CLIENT_PROTOCOL_41");

        int maxPacketSize = MysqlPacketBuffer.readInt4(in);
        int characterSet = MysqlPacketBuffer.readInt1(in);
        checkArgument(in.readableBytes() >= 23, "Truncated HandshakeResponse41 reserved bytes");
        in.skipBytes(23);

        String username = MysqlPacketBuffer.readNulTerminatedString(in);
        byte[] authResponse;
        if (cap.isPluginAuthLenencClientData()) {
            authResponse = MysqlPacketBuffer.readLenEncBytes(in);
        }
        else if (cap.isSecureConnection()) {
            int len = MysqlPacketBuffer.readInt1(in);
            checkArgument(len <= in.readableBytes(), "Auth response length %s exceeds remaining bytes %s", len, in.readableBytes());
            authResponse = new byte[len];
            in.readBytes(authResponse);
        }
        else {
            authResponse = MysqlPacketBuffer.readNulTerminatedBytes(in);
        }

        Optional<String> database = Optional.empty();
        if (cap.isConnectWithDb() && in.isReadable()) {
            String db = MysqlPacketBuffer.readNulTerminatedString(in);
            if (!db.isEmpty()) {
                database = Optional.of(db);
            }
        }

        Optional<String> authPluginName = Optional.empty();
        if (cap.isPluginAuth() && in.isReadable()) {
            String plugin = MysqlPacketBuffer.readNulTerminatedString(in);
            if (!plugin.isEmpty()) {
                authPluginName = Optional.of(plugin);
            }
        }

        ImmutableMap.Builder<String, String> attrs = ImmutableMap.builder();
        if (cap.isConnectAttrs() && in.isReadable()) {
            long totalLen = MysqlPacketBuffer.readLenEncInt(in);
            int endIndex = in.readerIndex() + (int) Math.min(totalLen, in.readableBytes());
            while (in.readerIndex() < endIndex) {
                String key = MysqlPacketBuffer.readLenEncString(in);
                String value = MysqlPacketBuffer.readLenEncString(in);
                attrs.put(key, value);
            }
        }

        return new HandshakeResponse41Packet(
                capabilities,
                maxPacketSize,
                characterSet,
                username,
                authResponse,
                database,
                authPluginName,
                attrs.buildKeepingLast());
    }
}
