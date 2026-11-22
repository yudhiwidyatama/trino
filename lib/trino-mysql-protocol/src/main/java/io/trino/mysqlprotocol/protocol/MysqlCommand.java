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

import java.util.Optional;

public enum MysqlCommand
{
    COM_SLEEP(0x00),
    COM_QUIT(0x01),
    COM_INIT_DB(0x02),
    COM_QUERY(0x03),
    COM_FIELD_LIST(0x04),
    COM_CREATE_DB(0x05),
    COM_DROP_DB(0x06),
    COM_REFRESH(0x07),
    COM_SHUTDOWN(0x08),
    COM_STATISTICS(0x09),
    COM_PROCESS_INFO(0x0A),
    COM_CONNECT(0x0B),
    COM_PROCESS_KILL(0x0C),
    COM_DEBUG(0x0D),
    COM_PING(0x0E),
    COM_TIME(0x0F),
    COM_DELAYED_INSERT(0x10),
    COM_CHANGE_USER(0x11),
    COM_STMT_PREPARE(0x16),
    COM_STMT_EXECUTE(0x17),
    COM_STMT_SEND_LONG_DATA(0x18),
    COM_STMT_CLOSE(0x19),
    COM_STMT_RESET(0x1A),
    COM_SET_OPTION(0x1B),
    COM_STMT_FETCH(0x1C),
    COM_RESET_CONNECTION(0x1F);

    private static final MysqlCommand[] BY_CODE = new MysqlCommand[32];

    static {
        for (MysqlCommand command : values()) {
            BY_CODE[command.code] = command;
        }
    }

    private final int code;

    MysqlCommand(int code)
    {
        this.code = code;
    }

    public int code()
    {
        return code;
    }

    public static Optional<MysqlCommand> fromCode(int code)
    {
        if (code < 0 || code >= BY_CODE.length) {
            return Optional.empty();
        }
        return Optional.ofNullable(BY_CODE[code]);
    }
}
