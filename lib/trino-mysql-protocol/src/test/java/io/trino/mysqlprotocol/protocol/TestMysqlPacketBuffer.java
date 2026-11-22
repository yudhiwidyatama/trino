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
import org.junit.jupiter.api.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestMysqlPacketBuffer
{
    @Test
    void testFixedLengthIntegers()
    {
        ByteBuf buf = Unpooled.buffer();
        try {
            MysqlPacketBuffer.writeInt1(buf, 0xAB);
            MysqlPacketBuffer.writeInt2(buf, 0x1234);
            MysqlPacketBuffer.writeInt3(buf, 0x123456);
            MysqlPacketBuffer.writeInt4(buf, 0x12345678);
            MysqlPacketBuffer.writeInt8(buf, 0x0102030405060708L);

            assertThat(MysqlPacketBuffer.readInt1(buf)).isEqualTo(0xAB);
            assertThat(MysqlPacketBuffer.readInt2(buf)).isEqualTo(0x1234);
            assertThat(MysqlPacketBuffer.readInt3(buf)).isEqualTo(0x123456);
            assertThat(MysqlPacketBuffer.readInt4(buf)).isEqualTo(0x12345678);
            assertThat(MysqlPacketBuffer.readInt8(buf)).isEqualTo(0x0102030405060708L);
            assertThat(buf.isReadable()).isFalse();
        }
        finally {
            buf.release();
        }
    }

    @Test
    void testLengthEncodedIntegers()
    {
        long[] values = {0L, 1L, 250L, 251L, 252L, 0xFFFFL, 0x10000L, 0xFFFFFFL, 0x1000000L, 0x123456789ABCL};
        ByteBuf buf = Unpooled.buffer();
        try {
            for (long value : values) {
                MysqlPacketBuffer.writeLenEncInt(buf, value);
            }
            for (long expected : values) {
                assertThat(MysqlPacketBuffer.readLenEncInt(buf)).isEqualTo(expected);
            }
            assertThat(buf.isReadable()).isFalse();
        }
        finally {
            buf.release();
        }
    }

    @Test
    void testStringsAndBytes()
    {
        ByteBuf buf = Unpooled.buffer();
        try {
            MysqlPacketBuffer.writeNulTerminatedString(buf, "trino_user");
            MysqlPacketBuffer.writeLenEncString(buf, "hello 世界");
            MysqlPacketBuffer.writeLenEncBytes(buf, new byte[] {1, 2, 3});
            MysqlPacketBuffer.writeNull(buf);
            MysqlPacketBuffer.writeEofString(buf, "trailing eof");

            assertThat(MysqlPacketBuffer.readNulTerminatedString(buf)).isEqualTo("trino_user");
            assertThat(MysqlPacketBuffer.readLenEncString(buf)).isEqualTo("hello 世界");
            assertThat(MysqlPacketBuffer.readLenEncBytes(buf)).containsExactly(1, 2, 3);
            assertThat(MysqlPacketBuffer.readInt1(buf)).isEqualTo(0xFB);
            assertThat(MysqlPacketBuffer.readEofString(buf)).isEqualTo("trailing eof");
            assertThat(buf.isReadable()).isFalse();
        }
        finally {
            buf.release();
        }
    }

    @Test
    void testOversizedLengthEncodedBytesRejected()
    {
        ByteBuf buf = Unpooled.buffer();
        try {
            // Claim 1000 bytes follow, but only provide 3 bytes
            MysqlPacketBuffer.writeLenEncInt(buf, 1000);
            buf.writeBytes("abc".getBytes(UTF_8));

            assertThatThrownBy(() -> MysqlPacketBuffer.readLenEncBytes(buf))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("exceeds remaining");
        }
        finally {
            buf.release();
        }
    }
}
