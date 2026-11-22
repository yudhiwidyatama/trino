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

import static java.util.Objects.requireNonNull;

/**
 * MySQL Protocol::AuthSwitchRequest packet (0xFE).
 */
public final class AuthSwitchRequestPacket
        implements MysqlPacket
{
    private final String pluginName;
    private final byte[] authPluginData;

    public AuthSwitchRequestPacket(String pluginName, byte[] authPluginData)
    {
        this.pluginName = requireNonNull(pluginName, "pluginName is null");
        this.authPluginData = requireNonNull(authPluginData, "authPluginData is null").clone();
    }

    @Override
    public void writeTo(ByteBuf out)
    {
        MysqlPacketBuffer.writeInt1(out, 0xFE);
        MysqlPacketBuffer.writeNulTerminatedString(out, pluginName);
        if (authPluginData.length > 0) {
            out.writeBytes(authPluginData);
            MysqlPacketBuffer.writeInt1(out, 0);
        }
    }
}
