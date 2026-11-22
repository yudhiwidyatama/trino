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
package io.trino.mysqlprotocol.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.trino.mysqlprotocol.server.MysqlPacketDecoder;
import io.trino.mysqlprotocol.server.MysqlPacketEncoder;
import io.trino.mysqlprotocol.server.MysqlSequenceId;
import org.junit.jupiter.api.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestMysqlPacketCodec
{
    @Test
    void testFragmentedSinglePacketDecode()
    {
        MysqlSequenceId sequenceId = new MysqlSequenceId();
        EmbeddedChannel channel = new EmbeddedChannel(new MysqlPacketDecoder(sequenceId, 1024 * 1024));
        try {
            byte[] payload = "SELECT 1".getBytes(UTF_8);
            ByteBuf wire = Unpooled.buffer();
            MysqlPacketBuffer.writeInt3(wire, payload.length);
            MysqlPacketBuffer.writeInt1(wire, 0);
            wire.writeBytes(payload);

            // Feed 1 byte at a time to verify incremental reassembly
            while (wire.isReadable()) {
                channel.writeInbound(wire.readRetainedSlice(1));
            }
            wire.release();

            ByteBuf decoded = channel.readInbound();
            assertThat(decoded).isNotNull();
            try {
                assertThat(decoded.toString(UTF_8)).isEqualTo("SELECT 1");
                assertThat(sequenceId.current()).isEqualTo(1);
            }
            finally {
                decoded.release();
            }
        }
        finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void testSequenceIdWrappingAndMismatch()
    {
        MysqlSequenceId sequenceId = new MysqlSequenceId();
        sequenceId.set(255);
        EmbeddedChannel channel = new EmbeddedChannel(new MysqlPacketDecoder(sequenceId, 1024));
        try {
            ByteBuf packet255 = Unpooled.buffer();
            MysqlPacketBuffer.writeInt3(packet255, 1);
            MysqlPacketBuffer.writeInt1(packet255, 255);
            packet255.writeByte(0x0E);
            assertThat(channel.writeInbound(packet255)).isTrue();
            ByteBuf decoded = channel.readInbound();
            decoded.release();
            assertThat(sequenceId.current()).isEqualTo(0);

            // Next packet with wrong sequence id (expects 0, sends 2)
            ByteBuf badSeq = Unpooled.buffer();
            MysqlPacketBuffer.writeInt3(badSeq, 1);
            MysqlPacketBuffer.writeInt1(badSeq, 2);
            badSeq.writeByte(0x0E);
            assertThatThrownBy(() -> channel.writeInbound(badSeq))
                    .hasMessageContaining("sequence");
        }
        finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void testMaxAllowedPacketRejected()
    {
        MysqlSequenceId sequenceId = new MysqlSequenceId();
        EmbeddedChannel channel = new EmbeddedChannel(new MysqlPacketDecoder(sequenceId, 16));
        try {
            ByteBuf oversized = Unpooled.buffer();
            MysqlPacketBuffer.writeInt3(oversized, 32);
            MysqlPacketBuffer.writeInt1(oversized, 0);
            oversized.writeZero(32);

            assertThatThrownBy(() -> channel.writeInbound(oversized))
                    .hasMessageContaining("max_allowed_packet");
        }
        finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void testMultiPacketSplitAndReassembly()
    {
        // Use small maxPhysicalPacketLength (8 bytes) via package/constructor overload for fast testing
        MysqlSequenceId encodeSeq = new MysqlSequenceId();
        MysqlSequenceId decodeSeq = new MysqlSequenceId();
        EmbeddedChannel encodeChannel = new EmbeddedChannel(new MysqlPacketEncoder(encodeSeq, 8));
        EmbeddedChannel decodeChannel = new EmbeddedChannel(new MysqlPacketDecoder(decodeSeq, 1024, 8));
        try {
            // Exact multiple of 8 (16 bytes) -> must emit 3 physical packets (8 + 8 + 0)
            byte[] exactMultiple = "0123456789ABCDEF".getBytes(UTF_8);
            encodeChannel.writeOutbound(Unpooled.wrappedBuffer(exactMultiple));

            ByteBuf wire = encodeChannel.readOutbound();
            assertThat(wire).isNotNull();
            // 3 headers (12 bytes) + 16 payload bytes = 28 bytes
            assertThat(wire.readableBytes()).isEqualTo(28);
            assertThat(encodeSeq.current()).isEqualTo(3);

            assertThat(decodeChannel.writeInbound(wire)).isTrue();
            ByteBuf decoded = decodeChannel.readInbound();
            assertThat(decoded).isNotNull();
            try {
                assertThat(decoded.toString(UTF_8)).isEqualTo("0123456789ABCDEF");
                assertThat(decodeSeq.current()).isEqualTo(3);
            }
            finally {
                decoded.release();
            }
        }
        finally {
            encodeChannel.finishAndReleaseAll();
            decodeChannel.finishAndReleaseAll();
        }
    }
}
