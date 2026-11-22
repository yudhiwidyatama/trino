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

public final class MysqlFieldType
{
    public static final int CHARSET_UTF8_GENERAL_CI = 33;
    public static final int CHARSET_UTF8MB4 = 45;
    public static final int CHARSET_BINARY = 63;

    public static final int FLAG_NOT_NULL = 0x0001;
    public static final int FLAG_BINARY = 0x0080;
    public static final int FLAG_NUM = 0x8000;

    public static final int MYSQL_TYPE_DECIMAL = 0x00;
    public static final int MYSQL_TYPE_TINY = 0x01;
    public static final int MYSQL_TYPE_SHORT = 0x02;
    public static final int MYSQL_TYPE_LONG = 0x03;
    public static final int MYSQL_TYPE_FLOAT = 0x04;
    public static final int MYSQL_TYPE_DOUBLE = 0x05;
    public static final int MYSQL_TYPE_NULL = 0x06;
    public static final int MYSQL_TYPE_TIMESTAMP = 0x07;
    public static final int MYSQL_TYPE_LONGLONG = 0x08;
    public static final int MYSQL_TYPE_INT24 = 0x09;
    public static final int MYSQL_TYPE_DATE = 0x0A;
    public static final int MYSQL_TYPE_TIME = 0x0B;
    public static final int MYSQL_TYPE_DATETIME = 0x0C;
    public static final int MYSQL_TYPE_YEAR = 0x0D;
    public static final int MYSQL_TYPE_VARCHAR = 0x0F;
    public static final int MYSQL_TYPE_BIT = 0x10;
    public static final int MYSQL_TYPE_JSON = 0xF5;
    public static final int MYSQL_TYPE_NEWDECIMAL = 0xF6;
    public static final int MYSQL_TYPE_BLOB = 0xFC;
    public static final int MYSQL_TYPE_VAR_STRING = 0xFD;
    public static final int MYSQL_TYPE_STRING = 0xFE;

    private MysqlFieldType() {}
}
