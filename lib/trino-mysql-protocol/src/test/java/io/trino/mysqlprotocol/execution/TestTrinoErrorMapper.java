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

import io.trino.client.QueryError;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TestTrinoErrorMapper
{
    @Test
    void testCommonTrinoErrorsMappedToMysqlErrorCodes()
    {
        TrinoErrorMapper.MysqlError syntax = TrinoErrorMapper.fromQueryError(
                new QueryError("line 1:1: mismatched input", null, 1, "SYNTAX_ERROR", "USER_ERROR", null, null));
        assertThat(syntax.errorCode()).isEqualTo(1064);
        assertThat(syntax.sqlState()).isEqualTo("42000");

        TrinoErrorMapper.MysqlError missingTable = TrinoErrorMapper.fromQueryError(
                new QueryError("Table does not exist", null, 46, "TABLE_NOT_FOUND", "USER_ERROR", null, null));
        assertThat(missingTable.errorCode()).isEqualTo(1146);
        assertThat(missingTable.sqlState()).isEqualTo("42S02");

        TrinoErrorMapper.MysqlError permissionDenied = TrinoErrorMapper.fromQueryError(
                new QueryError("Access denied", null, 4, "PERMISSION_DENIED", "USER_ERROR", null, null));
        assertThat(permissionDenied.errorCode()).isEqualTo(1142);
        assertThat(permissionDenied.sqlState()).isEqualTo("42000");

        TrinoErrorMapper.MysqlError canceled = TrinoErrorMapper.fromQueryError(
                new QueryError("Query was canceled", null, 65540, "USER_CANCELED", "USER_ERROR", null, null));
        assertThat(canceled.errorCode()).isEqualTo(1317);
        assertThat(canceled.sqlState()).isEqualTo("70100");
    }
}
