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
 * MySQL Protocol::ColumnDefinition41 packet.
 */
public final class ColumnDefinition41Packet
        implements MysqlPacket
{
    private final String schema;
    private final String table;
    private final String orgTable;
    private final String name;
    private final String orgName;
    private final int characterSet;
    private final int columnLength;
    private final int columnType;
    private final int flags;
    private final int decimals;

    public ColumnDefinition41Packet(
            String schema,
            String table,
            String orgTable,
            String name,
            String orgName,
            int characterSet,
            int columnLength,
            int columnType,
            int flags,
            int decimals)
    {
        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
        this.orgTable = requireNonNull(orgTable, "orgTable is null");
        this.name = requireNonNull(name, "name is null");
        this.orgName = requireNonNull(orgName, "orgName is null");
        this.characterSet = characterSet;
        this.columnLength = columnLength;
        this.columnType = columnType;
        this.flags = flags;
        this.decimals = decimals;
    }

    @Override
    public void writeTo(ByteBuf out)
    {
        MysqlPacketBuffer.writeLenEncString(out, "def");
        MysqlPacketBuffer.writeLenEncString(out, schema);
        MysqlPacketBuffer.writeLenEncString(out, table);
        MysqlPacketBuffer.writeLenEncString(out, orgTable);
        MysqlPacketBuffer.writeLenEncString(out, name);
        MysqlPacketBuffer.writeLenEncString(out, orgName);
        MysqlPacketBuffer.writeLenEncInt(out, 0x0C);
        MysqlPacketBuffer.writeInt2(out, characterSet);
        MysqlPacketBuffer.writeInt4(out, columnLength);
        MysqlPacketBuffer.writeInt1(out, columnType);
        MysqlPacketBuffer.writeInt2(out, flags);
        MysqlPacketBuffer.writeInt1(out, decimals);
        MysqlPacketBuffer.writeInt2(out, 0); // filler
    }
}
