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
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Decodes MySQL physical frames (3-byte LE length + 1-byte sequence id + payload)
 * and reassembles multi-packet logical payloads (&gt;= 16 MiB).
 */
public class MysqlPacketDecoder
        extends ByteToMessageDecoder
{
    public static final int PACKET_HEADER_LENGTH = 4;
    public static final int MAX_PHYSICAL_PACKET_LENGTH = 0xFFFFFF;

    private final MysqlSequenceId sequenceId;
    private final int maxAllowedPacket;
    private final int maxPhysicalPacketLength;
    private CompositeByteBuf assembledPayload;
    private long assembledLength;
    private boolean failed;

    public MysqlPacketDecoder(MysqlSequenceId sequenceId, int maxAllowedPacket)
    {
        this(sequenceId, maxAllowedPacket, MAX_PHYSICAL_PACKET_LENGTH);
    }

    public MysqlPacketDecoder(MysqlSequenceId sequenceId, int maxAllowedPacket, int maxPhysicalPacketLength)
    {
        this.sequenceId = requireNonNull(sequenceId, "sequenceId is null");
        checkArgument(maxAllowedPacket > 0, "maxAllowedPacket must be positive");
        checkArgument(maxPhysicalPacketLength > 0 && maxPhysicalPacketLength <= MAX_PHYSICAL_PACKET_LENGTH,
                "maxPhysicalPacketLength must be in (0, %s]", MAX_PHYSICAL_PACKET_LENGTH);
        this.maxAllowedPacket = maxAllowedPacket;
        this.maxPhysicalPacketLength = maxPhysicalPacketLength;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
    {
        if (failed) {
            in.skipBytes(in.readableBytes());
            return;
        }
        while (in.readableBytes() >= PACKET_HEADER_LENGTH) {
            in.markReaderIndex();
            int payloadLength = in.readUnsignedMediumLE();
            int packetSequenceId = in.readUnsignedByte();

            long totalLength = assembledLength + payloadLength;
            if (totalLength > maxAllowedPacket) {
                failed = true;
                in.skipBytes(in.readableBytes());
                releaseAssembled();
                throw new CorruptedFrameException("Packet size " + totalLength + " exceeds max_allowed_packet " + maxAllowedPacket);
            }

            if (in.readableBytes() < payloadLength) {
                in.resetReaderIndex();
                return;
            }

            int expectedSequenceId = sequenceId.current();
            if (packetSequenceId != expectedSequenceId) {
                failed = true;
                in.skipBytes(in.readableBytes());
                releaseAssembled();
                throw new CorruptedFrameException(
                        "Invalid MySQL packet sequence id: expected " + expectedSequenceId + ", got " + packetSequenceId);
            }
            sequenceId.nextAndIncrement();

            ByteBuf slice = in.readRetainedSlice(payloadLength);
            if (payloadLength == maxPhysicalPacketLength) {
                if (assembledPayload == null) {
                    assembledPayload = ctx.alloc().compositeBuffer();
                }
                assembledPayload.addComponent(true, slice);
                assembledLength += payloadLength;
                continue;
            }

            if (assembledPayload != null) {
                if (payloadLength > 0) {
                    assembledPayload.addComponent(true, slice);
                }
                else {
                    slice.release();
                }
                ByteBuf complete = assembledPayload;
                assembledPayload = null;
                assembledLength = 0;
                out.add(complete);
            }
            else {
                out.add(slice);
            }
        }
    }

    @Override
    protected void handlerRemoved0(ChannelHandlerContext ctx)
    {
        releaseAssembled();
    }

    private void releaseAssembled()
    {
        if (assembledPayload != null) {
            assembledPayload.release();
            assembledPayload = null;
            assembledLength = 0;
        }
    }
}
