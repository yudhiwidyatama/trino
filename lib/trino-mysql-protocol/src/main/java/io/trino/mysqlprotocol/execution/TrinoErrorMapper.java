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

import io.trino.client.ClientException;
import io.trino.client.QueryError;
import io.trino.mysqlprotocol.protocol.packet.ErrPacket;

public final class TrinoErrorMapper
{
    private TrinoErrorMapper() {}

    public record MysqlError(int errorCode, String sqlState, String message)
    {
        public ErrPacket toErrPacket()
        {
            return new ErrPacket(errorCode, sqlState, message);
        }
    }

    public static MysqlError fromQueryError(QueryError error)
    {
        if (error == null) {
            return new MysqlError(1105, "HY000", "Unknown Trino error");
        }
        String name = error.getErrorName() == null ? "" : error.getErrorName();
        String type = error.getErrorType() == null ? "" : error.getErrorType();
        String message = error.getMessage() == null || error.getMessage().isEmpty()
                ? name
                : error.getMessage();

        return switch (name) {
            case "SYNTAX_ERROR" -> new MysqlError(1064, "42000", message);
            case "CATALOG_NOT_FOUND", "SCHEMA_NOT_FOUND" -> new MysqlError(1049, "42000", message);
            case "TABLE_NOT_FOUND" -> new MysqlError(1146, "42S02", message);
            case "COLUMN_NOT_FOUND" -> new MysqlError(1054, "42S22", message);
            case "PERMISSION_DENIED" -> new MysqlError(1142, "42000", message);
            case "USER_CANCELED", "ADMINISTRATIVELY_KILLED", "ADMINISTRATIVELY_PREEMPTED" -> new MysqlError(1317, "70100", message);
            default -> {
                if ("INSUFFICIENT_RESOURCES".equals(type)) {
                    yield new MysqlError(1041, "HY000", message);
                }
                yield new MysqlError(1105, "HY000", message);
            }
        };
    }

    public static MysqlError fromThrowable(Throwable t)
    {
        String msg = t.getMessage() == null ? t.getClass().getSimpleName() : t.getMessage();
        if (t instanceof ClientException && msg.contains("401")) {
            return new MysqlError(1045, "28000", msg);
        }
        return new MysqlError(1105, "HY000", msg);
    }
}
