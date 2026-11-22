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
package io.trino.mysqlprotocol.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.mysqlprotocol.server.MysqlPacketDecoder.MAX_PHYSICAL_PACKET_LENGTH;
import static java.util.Objects.requireNonNull;

/**
 * Encodes logical MySQL packet payloads into physical frames (3-byte LE length + 1-byte sequence id + body),
 * splitting payloads &gt;= 16 MiB across multiple frames per the MySQL wire protocol.
 */
public class MysqlPacketEncoder
        extends MessageToByteEncoder<ByteBuf>
{
    private final MysqlSequenceId sequenceId;
    private final int maxPhysicalPacketLength;

    public MysqlPacketEncoder(MysqlSequenceId sequenceId)
    {
        this(sequenceId, MAX_PHYSICAL_PACKET_LENGTH);
    }

    public MysqlPacketEncoder(MysqlSequenceId sequenceId, int maxPhysicalPacketLength)
    {
        this.sequenceId = requireNonNull(sequenceId, "sequenceId is null");
        checkArgument(maxPhysicalPacketLength > 0 && maxPhysicalPacketLength <= MAX_PHYSICAL_PACKET_LENGTH,
                "maxPhysicalPacketLength must be in (0, %s]", MAX_PHYSICAL_PACKET_LENGTH);
        this.maxPhysicalPacketLength = maxPhysicalPacketLength;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, ByteBuf msg, ByteBuf out)
    {
        while (msg.readableBytes() >= maxPhysicalPacketLength) {
            out.writeMediumLE(maxPhysicalPacketLength);
            out.writeByte(sequenceId.nextAndIncrement());
            out.writeBytes(msg, maxPhysicalPacketLength);
        }
        int remaining = msg.readableBytes();
        out.writeMediumLE(remaining);
        out.writeByte(sequenceId.nextAndIncrement());
        if (remaining > 0) {
            out.writeBytes(msg, remaining);
        }
    }
}
