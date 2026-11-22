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
package io.trino.mysqlprotocol.execution;

import com.google.common.collect.ImmutableList;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.trino.client.ClientStandardTypes;
import io.trino.client.ClientTypeSignature;
import io.trino.client.ClientTypeSignatureParameter;
import io.trino.client.Column;
import io.trino.mysqlprotocol.protocol.MysqlFieldType;
import io.trino.mysqlprotocol.protocol.MysqlPacketBuffer;
import io.trino.mysqlprotocol.protocol.packet.ColumnDefinition41Packet;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class TestTrinoToMysqlTypeMapper
{
    @Test
    void testScalarTypeMappings()
    {
        assertTypeCode(ClientStandardTypes.BOOLEAN, MysqlFieldType.MYSQL_TYPE_TINY);
        assertTypeCode(ClientStandardTypes.TINYINT, MysqlFieldType.MYSQL_TYPE_TINY);
        assertTypeCode(ClientStandardTypes.SMALLINT, MysqlFieldType.MYSQL_TYPE_SHORT);
        assertTypeCode(ClientStandardTypes.INTEGER, MysqlFieldType.MYSQL_TYPE_LONG);
        assertTypeCode(ClientStandardTypes.BIGINT, MysqlFieldType.MYSQL_TYPE_LONGLONG);
        assertTypeCode(ClientStandardTypes.REAL, MysqlFieldType.MYSQL_TYPE_FLOAT);
        assertTypeCode(ClientStandardTypes.DOUBLE, MysqlFieldType.MYSQL_TYPE_DOUBLE);
        assertTypeCode(ClientStandardTypes.DATE, MysqlFieldType.MYSQL_TYPE_DATE);
        assertTypeCode(ClientStandardTypes.TIME, MysqlFieldType.MYSQL_TYPE_TIME);
        assertTypeCode(ClientStandardTypes.TIMESTAMP, MysqlFieldType.MYSQL_TYPE_DATETIME);
        assertTypeCode(ClientStandardTypes.VARCHAR, MysqlFieldType.MYSQL_TYPE_VAR_STRING);
        assertTypeCode(ClientStandardTypes.CHAR, MysqlFieldType.MYSQL_TYPE_STRING);
        assertTypeCode(ClientStandardTypes.VARBINARY, MysqlFieldType.MYSQL_TYPE_BLOB);
        assertTypeCode(ClientStandardTypes.JSON, MysqlFieldType.MYSQL_TYPE_JSON);
    }

    @Test
    void testDecimalPrecisionAndScale()
    {
        ClientTypeSignature decimalSig = new ClientTypeSignature(
                ClientStandardTypes.DECIMAL,
                ImmutableList.of(
                        ClientTypeSignatureParameter.ofLong(10),
                        ClientTypeSignatureParameter.ofLong(2)));
        Column column = new Column("price", "decimal(10,2)", decimalSig);
        ColumnDefinition41Packet def = TrinoToMysqlTypeMapper.toColumnDefinition("", "", column);

        ByteBuf buf = Unpooled.buffer();
        try {
            def.writeTo(buf);
            // skip 6 lenenc strings + 0x0C + charset(2) + length(4)
            for (int i = 0; i < 6; i++) {
                MysqlPacketBuffer.readLenEncString(buf);
            }
            MysqlPacketBuffer.readLenEncInt(buf);
            MysqlPacketBuffer.readInt2(buf);
            MysqlPacketBuffer.readInt4(buf);
            assertThat(MysqlPacketBuffer.readInt1(buf)).isEqualTo(MysqlFieldType.MYSQL_TYPE_NEWDECIMAL);
            MysqlPacketBuffer.readInt2(buf); // flags
            assertThat(MysqlPacketBuffer.readInt1(buf)).isEqualTo(2); // decimals = scale
        }
        finally {
            buf.release();
        }
    }

    @Test
    void testTextRowEncoding()
    {
        List<Column> columns = ImmutableList.of(
                new Column("b", "boolean", new ClientTypeSignature(ClientStandardTypes.BOOLEAN)),
                new Column("i", "bigint", new ClientTypeSignature(ClientStandardTypes.BIGINT)),
                new Column("s", "varchar", new ClientTypeSignature(ClientStandardTypes.VARCHAR)),
                new Column("n", "varchar", new ClientTypeSignature(ClientStandardTypes.VARCHAR)),
                new Column("bin", "varbinary", new ClientTypeSignature(ClientStandardTypes.VARBINARY)));

        List<Object> row = Arrays.asList(Boolean.TRUE, 42L, "hello", null, "abc".getBytes(UTF_8));
        ByteBuf buf = Unpooled.buffer();
        try {
            MysqlTextResultEncoder.encodeRow(buf, columns, row);
            assertThat(MysqlPacketBuffer.readLenEncString(buf)).isEqualTo("1");
            assertThat(MysqlPacketBuffer.readLenEncString(buf)).isEqualTo("42");
            assertThat(MysqlPacketBuffer.readLenEncString(buf)).isEqualTo("hello");
            assertThat(MysqlPacketBuffer.readInt1(buf)).isEqualTo(MysqlPacketBuffer.NULL_INDICATOR);
            assertThat(MysqlPacketBuffer.readLenEncBytes(buf)).containsExactly('a', 'b', 'c');
            assertThat(buf.isReadable()).isFalse();
        }
        finally {
            buf.release();
        }
    }

    private static void assertTypeCode(String trinoRawType, int expectedMysqlCode)
    {
        Column col = new Column("c", trinoRawType, new ClientTypeSignature(trinoRawType));
        ColumnDefinition41Packet def = TrinoToMysqlTypeMapper.toColumnDefinition("", "", col);
        ByteBuf buf = Unpooled.buffer();
        try {
            def.writeTo(buf);
            for (int i = 0; i < 6; i++) {
                MysqlPacketBuffer.readLenEncString(buf);
            }
            MysqlPacketBuffer.readLenEncInt(buf);
            MysqlPacketBuffer.readInt2(buf);
            MysqlPacketBuffer.readInt4(buf);
            assertThat(MysqlPacketBuffer.readInt1(buf)).isEqualTo(expectedMysqlCode);
        }
        finally {
            buf.release();
        }
    }
}
