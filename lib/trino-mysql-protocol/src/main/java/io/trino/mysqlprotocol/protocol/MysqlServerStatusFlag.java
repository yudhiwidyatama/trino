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

public final class MysqlServerStatusFlag
{
    public static final int SERVER_STATUS_IN_TRANS = 0x0001;
    public static final int SERVER_STATUS_AUTOCOMMIT = 0x0002;
    public static final int SERVER_MORE_RESULTS_EXISTS = 0x0008;
    public static final int SERVER_STATUS_CURSOR_EXISTS = 0x0040;
    public static final int SERVER_STATUS_LAST_ROW_SENT = 0x0080;

    private MysqlServerStatusFlag() {}
}
