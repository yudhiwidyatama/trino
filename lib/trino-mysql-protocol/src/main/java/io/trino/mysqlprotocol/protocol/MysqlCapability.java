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

public final class MysqlCapability
{
    public static final int CLIENT_LONG_PASSWORD = 0x0000_0001;
    public static final int CLIENT_FOUND_ROWS = 0x0000_0002;
    public static final int CLIENT_LONG_FLAG = 0x0000_0004;
    public static final int CLIENT_CONNECT_WITH_DB = 0x0000_0008;
    public static final int CLIENT_NO_SCHEMA = 0x0000_0010;
    public static final int CLIENT_COMPRESS = 0x0000_0020;
    public static final int CLIENT_ODBC = 0x0000_0040;
    public static final int CLIENT_LOCAL_FILES = 0x0000_0080;
    public static final int CLIENT_IGNORE_SPACE = 0x0000_0100;
    public static final int CLIENT_PROTOCOL_41 = 0x0000_0200;
    public static final int CLIENT_INTERACTIVE = 0x0000_0400;
    public static final int CLIENT_SSL = 0x0000_0800;
    public static final int CLIENT_IGNORE_SIGPIPE = 0x0000_1000;
    public static final int CLIENT_TRANSACTIONS = 0x0000_2000;
    public static final int CLIENT_RESERVED = 0x0000_4000;
    public static final int CLIENT_SECURE_CONNECTION = 0x0000_8000;
    public static final int CLIENT_MULTI_STATEMENTS = 0x0001_0000;
    public static final int CLIENT_MULTI_RESULTS = 0x0002_0000;
    public static final int CLIENT_PS_MULTI_RESULTS = 0x0004_0000;
    public static final int CLIENT_PLUGIN_AUTH = 0x0008_0000;
    public static final int CLIENT_CONNECT_ATTRS = 0x0010_0000;
    public static final int CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x0020_0000;
    public static final int CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS = 0x0040_0000;
    public static final int CLIENT_SESSION_TRACK = 0x0080_0000;
    public static final int CLIENT_DEPRECATE_EOF = 0x0100_0000;

    public static final int DEFAULT_SERVER_CAPABILITIES = CLIENT_LONG_PASSWORD
            | CLIENT_LONG_FLAG
            | CLIENT_CONNECT_WITH_DB
            | CLIENT_PROTOCOL_41
            | CLIENT_TRANSACTIONS
            | CLIENT_SECURE_CONNECTION
            | CLIENT_MULTI_RESULTS
            | CLIENT_PLUGIN_AUTH
            | CLIENT_CONNECT_ATTRS
            | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
            | CLIENT_DEPRECATE_EOF;

    private final int flags;

    public MysqlCapability(int flags)
    {
        this.flags = flags;
    }

    public int flags()
    {
        return flags;
    }

    public boolean has(int flag)
    {
        return (flags & flag) != 0;
    }

    public boolean isProtocol41()
    {
        return has(CLIENT_PROTOCOL_41);
    }

    public boolean isSsl()
    {
        return has(CLIENT_SSL);
    }

    public boolean isConnectWithDb()
    {
        return has(CLIENT_CONNECT_WITH_DB);
    }

    public boolean isPluginAuth()
    {
        return has(CLIENT_PLUGIN_AUTH);
    }

    public boolean isPluginAuthLenencClientData()
    {
        return has(CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA);
    }

    public boolean isSecureConnection()
    {
        return has(CLIENT_SECURE_CONNECTION);
    }

    public boolean isConnectAttrs()
    {
        return has(CLIENT_CONNECT_ATTRS);
    }

    public boolean isDeprecatedEof()
    {
        return has(CLIENT_DEPRECATE_EOF);
    }

    public boolean isSessionTrack()
    {
        return has(CLIENT_SESSION_TRACK);
    }
}
