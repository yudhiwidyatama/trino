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

import io.trino.client.ClientStandardTypes;
import io.trino.client.ClientTypeSignature;
import io.trino.client.ClientTypeSignatureParameter;
import io.trino.client.ClientTypeSignatureParameter.ParameterKind;
import io.trino.client.Column;
import io.trino.mysqlprotocol.protocol.MysqlFieldType;
import io.trino.mysqlprotocol.protocol.packet.ColumnDefinition41Packet;

import java.util.List;
import java.util.Locale;

import static io.trino.mysqlprotocol.protocol.MysqlFieldType.CHARSET_BINARY;
import static io.trino.mysqlprotocol.protocol.MysqlFieldType.CHARSET_UTF8MB4;
import static io.trino.mysqlprotocol.protocol.MysqlFieldType.FLAG_BINARY;
import static io.trino.mysqlprotocol.protocol.MysqlFieldType.FLAG_NUM;
import static io.trino.mysqlprotocol.protocol.MysqlFieldType.MYSQL_TYPE_BLOB;
import static io.trino.mysqlprotocol.protocol.MysqlFieldType.MYSQL_TYPE_DATE;
import static io.trino.mysqlprotocol.protocol.MysqlFieldType.MYSQL_TYPE_DATETIME;
import static io.trino.mysqlprotocol.protocol.MysqlFieldType.MYSQL_TYPE_DOUBLE;
import static io.trino.mysqlprotocol.protocol.MysqlFieldType.MYSQL_TYPE_FLOAT;
import static io.trino.mysqlprotocol.protocol.MysqlFieldType.MYSQL_TYPE_JSON;
import static io.trino.mysqlprotocol.protocol.MysqlFieldType.MYSQL_TYPE_LONG;
import static io.trino.mysqlprotocol.protocol.MysqlFieldType.MYSQL_TYPE_LONGLONG;
import static io.trino.mysqlprotocol.protocol.MysqlFieldType.MYSQL_TYPE_NEWDECIMAL;
import static io.trino.mysqlprotocol.protocol.MysqlFieldType.MYSQL_TYPE_SHORT;
import static io.trino.mysqlprotocol.protocol.MysqlFieldType.MYSQL_TYPE_STRING;
import static io.trino.mysqlprotocol.protocol.MysqlFieldType.MYSQL_TYPE_TIME;
import static io.trino.mysqlprotocol.protocol.MysqlFieldType.MYSQL_TYPE_TINY;
import static io.trino.mysqlprotocol.protocol.MysqlFieldType.MYSQL_TYPE_VAR_STRING;
import static java.lang.Math.toIntExact;

public final class TrinoToMysqlTypeMapper
{
    private static final int DEFAULT_STRING_LENGTH = 65535;

    private TrinoToMysqlTypeMapper() {}

    public static ColumnDefinition41Packet toColumnDefinition(String schema, String table, Column column)
    {
        ClientTypeSignature signature = column.getTypeSignature();
        String rawType = signature != null ? signature.getRawType().toLowerCase(Locale.ROOT) : column.getType().toLowerCase(Locale.ROOT);
        List<ClientTypeSignatureParameter> args = signature != null ? signature.getArguments() : List.of();

        int mysqlType;
        int charset = CHARSET_UTF8MB4;
        int length = DEFAULT_STRING_LENGTH;
        int flags = 0;
        int decimals = 0;

        switch (rawType) {
            case ClientStandardTypes.BOOLEAN -> {
                mysqlType = MYSQL_TYPE_TINY;
                charset = CHARSET_BINARY;
                length = 1;
                flags = FLAG_NUM;
            }
            case ClientStandardTypes.TINYINT -> {
                mysqlType = MYSQL_TYPE_TINY;
                charset = CHARSET_BINARY;
                length = 4;
                flags = FLAG_NUM;
            }
            case ClientStandardTypes.SMALLINT -> {
                mysqlType = MYSQL_TYPE_SHORT;
                charset = CHARSET_BINARY;
                length = 6;
                flags = FLAG_NUM;
            }
            case ClientStandardTypes.INTEGER -> {
                mysqlType = MYSQL_TYPE_LONG;
                charset = CHARSET_BINARY;
                length = 11;
                flags = FLAG_NUM;
            }
            case ClientStandardTypes.BIGINT -> {
                mysqlType = MYSQL_TYPE_LONGLONG;
                charset = CHARSET_BINARY;
                length = 20;
                flags = FLAG_NUM;
            }
            case ClientStandardTypes.REAL -> {
                mysqlType = MYSQL_TYPE_FLOAT;
                charset = CHARSET_BINARY;
                length = 12;
                flags = FLAG_NUM;
                decimals = 31;
            }
            case ClientStandardTypes.DOUBLE -> {
                mysqlType = MYSQL_TYPE_DOUBLE;
                charset = CHARSET_BINARY;
                length = 22;
                flags = FLAG_NUM;
                decimals = 31;
            }
            case ClientStandardTypes.DECIMAL -> {
                mysqlType = MYSQL_TYPE_NEWDECIMAL;
                charset = CHARSET_BINARY;
                int precision = args.size() >= 1 && args.get(0).getKind() == ParameterKind.LONG
                        ? toIntExact(args.get(0).getLongLiteral()) : 38;
                int scale = args.size() >= 2 && args.get(1).getKind() == ParameterKind.LONG
                        ? toIntExact(args.get(1).getLongLiteral()) : 0;
                length = precision + (scale > 0 ? 2 : 1);
                flags = FLAG_NUM;
                decimals = scale;
            }
            case ClientStandardTypes.DATE -> {
                mysqlType = MYSQL_TYPE_DATE;
                charset = CHARSET_BINARY;
                length = 10;
            }
            case ClientStandardTypes.TIME -> {
                mysqlType = MYSQL_TYPE_TIME;
                charset = CHARSET_BINARY;
                int p = args.size() >= 1 && args.get(0).getKind() == ParameterKind.LONG
                        ? toIntExact(args.get(0).getLongLiteral()) : 3;
                decimals = Math.min(6, Math.max(0, p));
                length = decimals > 0 ? 9 + decimals : 8;
            }
            case ClientStandardTypes.TIMESTAMP -> {
                mysqlType = MYSQL_TYPE_DATETIME;
                charset = CHARSET_BINARY;
                int p = args.size() >= 1 && args.get(0).getKind() == ParameterKind.LONG
                        ? toIntExact(args.get(0).getLongLiteral()) : 3;
                decimals = Math.min(6, Math.max(0, p));
                length = decimals > 0 ? 20 + decimals : 19;
            }
            case ClientStandardTypes.CHAR -> {
                mysqlType = MYSQL_TYPE_STRING;
                length = extractStringLength(args);
            }
            case ClientStandardTypes.VARCHAR -> {
                mysqlType = MYSQL_TYPE_VAR_STRING;
                length = extractStringLength(args);
            }
            case ClientStandardTypes.VARBINARY -> {
                mysqlType = MYSQL_TYPE_BLOB;
                charset = CHARSET_BINARY;
                flags = FLAG_BINARY;
                length = DEFAULT_STRING_LENGTH;
            }
            case ClientStandardTypes.JSON -> {
                mysqlType = MYSQL_TYPE_JSON;
                length = DEFAULT_STRING_LENGTH;
            }
            default -> {
                mysqlType = MYSQL_TYPE_VAR_STRING;
                length = DEFAULT_STRING_LENGTH;
            }
        }

        return new ColumnDefinition41Packet(
                schema,
                table,
                table,
                column.getName(),
                column.getName(),
                charset,
                length,
                mysqlType,
                flags,
                decimals);
    }

    public static ColumnDefinition41Packet stringColumn(String name)
    {
        return stringColumn("", "", name);
    }

    public static ColumnDefinition41Packet stringColumn(String schema, String table, String name)
    {
        return new ColumnDefinition41Packet(
                schema,
                table,
                table,
                name,
                name,
                CHARSET_UTF8MB4,
                DEFAULT_STRING_LENGTH,
                MysqlFieldType.MYSQL_TYPE_VAR_STRING,
                0,
                0);
    }

    private static int extractStringLength(List<ClientTypeSignatureParameter> args)
    {
        if (!args.isEmpty() && args.get(0).getKind() == ParameterKind.LONG) {
            long declared = args.get(0).getLongLiteral();
            if (declared > 0 && declared <= Integer.MAX_VALUE / 4) {
                return (int) (declared * 4L);
            }
        }
        return DEFAULT_STRING_LENGTH;
    }
}
