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
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Objects.requireNonNull;

/**
 * MySQL Protocol::ERR_Packet (header 0xFF).
 */
public final class ErrPacket
        implements MysqlPacket
{
    private final int errorCode;
    private final String sqlState;
    private final String message;

    public ErrPacket(int errorCode, String sqlState, String message)
    {
        this.errorCode = errorCode;
        this.sqlState = requireNonNull(sqlState, "sqlState is null");
        checkArgument(sqlState.length() == 5, "sqlState must be 5 characters: %s", sqlState);
        this.message = (message == null || message.isEmpty()) ? "Unknown error" : message;
    }

    @Override
    public void writeTo(ByteBuf out)
    {
        MysqlPacketBuffer.writeInt1(out, 0xFF);
        MysqlPacketBuffer.writeInt2(out, errorCode);
        MysqlPacketBuffer.writeInt1(out, '#');
        out.writeBytes(sqlState.getBytes(US_ASCII));
        MysqlPacketBuffer.writeEofString(out, message);
    }
}
