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

import io.netty.buffer.ByteBuf;
import io.trino.client.Column;
import io.trino.client.Row;
import io.trino.client.RowField;
import io.trino.mysqlprotocol.protocol.MysqlPacketBuffer;

import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Encodes Trino QueryData rows (normalized by trino-client's FixJsonDataUtils) into MySQL Text Resultset Row packets.
 */
public final class MysqlTextResultEncoder
{
    private MysqlTextResultEncoder() {}

    public static void encodeRow(ByteBuf out, List<Column> columns, List<Object> row)
    {
        for (int i = 0; i < row.size(); i++) {
            Object value = row.get(i);
            if (value == null) {
                MysqlPacketBuffer.writeNull(out);
            }
            else if (value instanceof byte[] bytes) {
                MysqlPacketBuffer.writeLenEncBytes(out, bytes);
            }
            else if (value instanceof Boolean bool) {
                MysqlPacketBuffer.writeLenEncString(out, bool ? "1" : "0");
            }
            else {
                MysqlPacketBuffer.writeLenEncString(out, formatValue(value));
            }
        }
    }

    public static void encodeStringRow(ByteBuf out, List<String> row)
    {
        for (String value : row) {
            if (value == null) {
                MysqlPacketBuffer.writeNull(out);
            }
            else {
                MysqlPacketBuffer.writeLenEncString(out, value);
            }
        }
    }

    private static String formatValue(Object value)
    {
        if (value instanceof Row row) {
            StringJoiner joiner = new StringJoiner(", ", "{", "}");
            for (RowField field : row.getFields()) {
                String name = field.getName().orElse("");
                String formatted = field.getValue() == null ? "null" : formatValue(field.getValue());
                joiner.add(name.isEmpty() ? formatted : name + "=" + formatted);
            }
            return joiner.toString();
        }
        if (value instanceof List<?> list) {
            StringJoiner joiner = new StringJoiner(", ", "[", "]");
            for (Object elem : list) {
                joiner.add(elem == null ? "null" : formatValue(elem));
            }
            return joiner.toString();
        }
        if (value instanceof Map<?, ?> map) {
            StringJoiner joiner = new StringJoiner(", ", "{", "}");
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                joiner.add(formatValue(entry.getKey()) + "=" + (entry.getValue() == null ? "null" : formatValue(entry.getValue())));
            }
            return joiner.toString();
        }
        return value.toString();
    }
}
