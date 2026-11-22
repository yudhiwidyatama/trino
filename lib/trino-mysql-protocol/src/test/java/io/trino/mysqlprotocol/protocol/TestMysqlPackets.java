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
import io.trino.mysqlprotocol.protocol.packet.ColumnDefinition41Packet;
import io.trino.mysqlprotocol.protocol.packet.EofPacket;
import io.trino.mysqlprotocol.protocol.packet.ErrPacket;
import io.trino.mysqlprotocol.protocol.packet.HandshakeResponse41Packet;
import io.trino.mysqlprotocol.protocol.packet.HandshakeV10Packet;
import io.trino.mysqlprotocol.protocol.packet.OkPacket;
import io.trino.mysqlprotocol.protocol.packet.ResultSetEndPacket;
import org.junit.jupiter.api.Test;

import static io.trino.mysqlprotocol.protocol.MysqlCapability.CLIENT_CONNECT_ATTRS;
import static io.trino.mysqlprotocol.protocol.MysqlCapability.CLIENT_CONNECT_WITH_DB;
import static io.trino.mysqlprotocol.protocol.MysqlCapability.CLIENT_DEPRECATE_EOF;
import static io.trino.mysqlprotocol.protocol.MysqlCapability.CLIENT_PLUGIN_AUTH;
import static io.trino.mysqlprotocol.protocol.MysqlCapability.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;
import static io.trino.mysqlprotocol.protocol.MysqlCapability.CLIENT_PROTOCOL_41;
import static io.trino.mysqlprotocol.protocol.MysqlCapability.CLIENT_SECURE_CONNECTION;
import static org.assertj.core.api.Assertions.assertThat;

class TestMysqlPackets
{
    @Test
    void testHandshakeV10Serialization()
    {
        byte[] scramble = new byte[20];
        for (int i = 0; i < 20; i++) {
            scramble[i] = (byte) (i + 1);
        }
        HandshakeV10Packet packet = new HandshakeV10Packet(
                "8.0.32-trino",
                42,
                scramble,
                MysqlCapability.DEFAULT_SERVER_CAPABILITIES,
                MysqlServerStatusFlag.SERVER_STATUS_AUTOCOMMIT,
                HandshakeV10Packet.MYSQL_NATIVE_PASSWORD);

        ByteBuf buf = Unpooled.buffer();
        try {
            packet.writeTo(buf);

            assertThat(MysqlPacketBuffer.readInt1(buf)).isEqualTo(10);
            assertThat(MysqlPacketBuffer.readNulTerminatedString(buf)).isEqualTo("8.0.32-trino");
            assertThat(MysqlPacketBuffer.readInt4(buf)).isEqualTo(42);
            byte[] part1 = new byte[8];
            buf.readBytes(part1);
            assertThat(part1).containsExactly(1, 2, 3, 4, 5, 6, 7, 8);
            assertThat(MysqlPacketBuffer.readInt1(buf)).isEqualTo(0); // filler
            int lowerCap = MysqlPacketBuffer.readInt2(buf);
            assertThat(MysqlPacketBuffer.readInt1(buf)).isEqualTo(MysqlFieldType.CHARSET_UTF8MB4);
            assertThat(MysqlPacketBuffer.readInt2(buf)).isEqualTo(MysqlServerStatusFlag.SERVER_STATUS_AUTOCOMMIT);
            int upperCap = MysqlPacketBuffer.readInt2(buf);
            assertThat(lowerCap | (upperCap << 16)).isEqualTo(MysqlCapability.DEFAULT_SERVER_CAPABILITIES);
            assertThat(MysqlPacketBuffer.readInt1(buf)).isEqualTo(21);
            buf.skipBytes(10); // reserved
            byte[] part2 = new byte[12];
            buf.readBytes(part2);
            assertThat(part2).containsExactly(9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20);
            assertThat(MysqlPacketBuffer.readInt1(buf)).isEqualTo(0); // trailing NUL for scramble
            assertThat(MysqlPacketBuffer.readNulTerminatedString(buf)).isEqualTo("mysql_native_password");
            assertThat(buf.isReadable()).isFalse();
        }
        finally {
            buf.release();
        }
    }

    @Test
    void testHandshakeResponse41Deserialization()
    {
        int flags = CLIENT_PROTOCOL_41
                | CLIENT_SECURE_CONNECTION
                | CLIENT_CONNECT_WITH_DB
                | CLIENT_PLUGIN_AUTH
                | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
                | CLIENT_CONNECT_ATTRS;

        ByteBuf buf = Unpooled.buffer();
        try {
            MysqlPacketBuffer.writeInt4(buf, flags);
            MysqlPacketBuffer.writeInt4(buf, 16 * 1024 * 1024);
            MysqlPacketBuffer.writeInt1(buf, MysqlFieldType.CHARSET_UTF8MB4);
            buf.writeZero(23);
            MysqlPacketBuffer.writeNulTerminatedString(buf, "alice");
            MysqlPacketBuffer.writeLenEncBytes(buf, new byte[] {10, 20, 30});
            MysqlPacketBuffer.writeNulTerminatedString(buf, "tpch.tiny");
            MysqlPacketBuffer.writeNulTerminatedString(buf, "mysql_native_password");

            ByteBuf attrs = Unpooled.buffer();
            MysqlPacketBuffer.writeLenEncString(attrs, "_client_name");
            MysqlPacketBuffer.writeLenEncString(attrs, "mysql-connector-j");
            MysqlPacketBuffer.writeLenEncInt(buf, attrs.readableBytes());
            buf.writeBytes(attrs);
            attrs.release();

            HandshakeResponse41Packet parsed = HandshakeResponse41Packet.readFrom(buf);
            assertThat(parsed.capabilities()).isEqualTo(flags);
            assertThat(parsed.username()).isEqualTo("alice");
            assertThat(parsed.authResponse()).containsExactly(10, 20, 30);
            assertThat(parsed.database()).contains("tpch.tiny");
            assertThat(parsed.authPluginName()).contains("mysql_native_password");
            assertThat(parsed.connectAttributes()).containsEntry("_client_name", "mysql-connector-j");
        }
        finally {
            buf.release();
        }
    }

    @Test
    void testOkErrEofAndResultSetEndPackets()
    {
        ByteBuf buf = Unpooled.buffer();
        try {
            // OK packet with CLIENT_DEPRECATE_EOF
            new OkPacket(5, 0, MysqlServerStatusFlag.SERVER_STATUS_AUTOCOMMIT, 1, "", CLIENT_PROTOCOL_41 | CLIENT_DEPRECATE_EOF)
                    .writeTo(buf);
            assertThat(MysqlPacketBuffer.readInt1(buf)).isEqualTo(0x00);
            assertThat(MysqlPacketBuffer.readLenEncInt(buf)).isEqualTo(5L);
            assertThat(MysqlPacketBuffer.readLenEncInt(buf)).isEqualTo(0L);
            assertThat(MysqlPacketBuffer.readInt2(buf)).isEqualTo(MysqlServerStatusFlag.SERVER_STATUS_AUTOCOMMIT);
            assertThat(MysqlPacketBuffer.readInt2(buf)).isEqualTo(1);
            assertThat(MysqlPacketBuffer.readLenEncInt(buf)).isEqualTo(0L); // empty info lenenc
            assertThat(buf.isReadable()).isFalse();

            // ERR packet
            buf.clear();
            new ErrPacket(1064, "42000", "Syntax error").writeTo(buf);
            assertThat(MysqlPacketBuffer.readInt1(buf)).isEqualTo(0xFF);
            assertThat(MysqlPacketBuffer.readInt2(buf)).isEqualTo(1064);
            assertThat((char) MysqlPacketBuffer.readInt1(buf)).isEqualTo('#');
            byte[] sqlState = new byte[5];
            buf.readBytes(sqlState);
            assertThat(new String(sqlState, java.nio.charset.StandardCharsets.US_ASCII)).isEqualTo("42000");
            assertThat(MysqlPacketBuffer.readEofString(buf)).isEqualTo("Syntax error");

            // Legacy EOF packet (5 bytes total)
            buf.clear();
            new EofPacket(0, MysqlServerStatusFlag.SERVER_STATUS_AUTOCOMMIT).writeTo(buf);
            assertThat(buf.readableBytes()).isEqualTo(5);
            assertThat(MysqlPacketBuffer.readInt1(buf)).isEqualTo(0xFE);

            // ResultSetEndPacket for CLIENT_DEPRECATE_EOF (must be > 5 bytes with 0xFE header)
            buf.clear();
            new ResultSetEndPacket(MysqlServerStatusFlag.SERVER_STATUS_AUTOCOMMIT, 0).writeTo(buf);
            assertThat(buf.readableBytes()).isGreaterThan(5);
            assertThat(MysqlPacketBuffer.readInt1(buf)).isEqualTo(0xFE);
        }
        finally {
            buf.release();
        }
    }

    @Test
    void testColumnDefinition41Packet()
    {
        ColumnDefinition41Packet col = new ColumnDefinition41Packet(
                "tiny",
                "nation",
                "nation",
                "nationkey",
                "nationkey",
                MysqlFieldType.CHARSET_BINARY,
                20,
                MysqlFieldType.MYSQL_TYPE_LONGLONG,
                0,
                0);

        ByteBuf buf = Unpooled.buffer();
        try {
            col.writeTo(buf);
            assertThat(MysqlPacketBuffer.readLenEncString(buf)).isEqualTo("def");
            assertThat(MysqlPacketBuffer.readLenEncString(buf)).isEqualTo("tiny");
            assertThat(MysqlPacketBuffer.readLenEncString(buf)).isEqualTo("nation");
            assertThat(MysqlPacketBuffer.readLenEncString(buf)).isEqualTo("nation");
            assertThat(MysqlPacketBuffer.readLenEncString(buf)).isEqualTo("nationkey");
            assertThat(MysqlPacketBuffer.readLenEncString(buf)).isEqualTo("nationkey");
            assertThat(MysqlPacketBuffer.readLenEncInt(buf)).isEqualTo(0x0CL);
            assertThat(MysqlPacketBuffer.readInt2(buf)).isEqualTo(MysqlFieldType.CHARSET_BINARY);
            assertThat(MysqlPacketBuffer.readInt4(buf)).isEqualTo(20);
            assertThat(MysqlPacketBuffer.readInt1(buf)).isEqualTo(MysqlFieldType.MYSQL_TYPE_LONGLONG);
            assertThat(MysqlPacketBuffer.readInt2(buf)).isEqualTo(0);
            assertThat(MysqlPacketBuffer.readInt1(buf)).isEqualTo(0);
            assertThat(MysqlPacketBuffer.readInt2(buf)).isEqualTo(0);
            assertThat(buf.isReadable()).isFalse();
        }
        finally {
            buf.release();
        }
    }
}
