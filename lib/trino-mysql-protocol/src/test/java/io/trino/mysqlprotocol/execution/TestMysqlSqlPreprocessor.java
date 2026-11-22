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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TestMysqlSqlPreprocessor
{
    @Test
    void testBackquotedIdentifiersRewrittenToDoubleQuotes()
    {
        assertThat(MysqlSqlPreprocessor.preprocess("SELECT `id`, `user_name` FROM `my_schema`.`my_table`"))
                .isEqualTo("SELECT \"id\", \"user_name\" FROM \"my_schema\".\"my_table\"");

        assertThat(MysqlSqlPreprocessor.preprocess("SHOW COLUMNS FROM `orders`"))
                .isEqualTo("SHOW COLUMNS FROM \"orders\"");

        assertThat(MysqlSqlPreprocessor.preprocess("SHOW CREATE TABLE `orders`"))
                .isEqualTo("SHOW CREATE TABLE \"orders\"");
    }

    @Test
    void testEscapedBackticksAndEmbeddedDoubleQuotesInIdentifiers()
    {
        assertThat(MysqlSqlPreprocessor.preprocess("SELECT `a``b`, `c\"d` FROM `tbl`"))
                .isEqualTo("SELECT \"a`b\", \"c\"\"d\" FROM \"tbl\"");
    }

    @Test
    void testBackticksInsideStringLiteralsAndCommentsPreserved()
    {
        assertThat(MysqlSqlPreprocessor.preprocess("SELECT 'hello `world`' AS `col_name` FROM `tbl`"))
                .isEqualTo("SELECT 'hello `world`' AS \"col_name\" FROM \"tbl\"");

        assertThat(MysqlSqlPreprocessor.preprocess("SELECT \"already `double` quoted\", `backquoted` FROM /* `comment` */ `tbl` -- `line_comment`"))
                .isEqualTo("SELECT \"already `double` quoted\", \"backquoted\" FROM /* `comment` */ \"tbl\" -- `line_comment`");
    }

    @Test
    void testBackslashEscapedSingleQuotesInStringLiterals()
    {
        assertThat(MysqlSqlPreprocessor.preprocess("SELECT * FROM `users` WHERE `name` = 'O\\'Reilly' AND `literal_tick` = '`kept`'"))
                .isEqualTo("SELECT * FROM \"users\" WHERE \"name\" = 'O''Reilly' AND \"literal_tick\" = '`kept`'");
    }

    @Test
    void testMySqlHashCommentsAndTrailingSemicolons()
    {
        assertThat(MysqlSqlPreprocessor.preprocess("SELECT `id` FROM `tbl` # comment with `backtick`\nWHERE `id` = 1;"))
                .isEqualTo("SELECT \"id\" FROM \"tbl\" --  comment with `backtick`\nWHERE \"id\" = 1");
    }

    @Test
    void testMySqlLimitOffsetRewrittenToTrinoAnsiOffsetLimit()
    {
        assertThat(MysqlSqlPreprocessor.preprocess("SELECT * FROM `my_table` LIMIT 0, 1000;"))
                .isEqualTo("SELECT * FROM \"my_table\" OFFSET 0 LIMIT 1000");

        assertThat(MysqlSqlPreprocessor.preprocess("SELECT `a`, `b` FROM `my_table` ORDER BY `a` ASC LIMIT 50, 25"))
                .isEqualTo("SELECT \"a\", \"b\" FROM \"my_table\" ORDER BY \"a\" ASC OFFSET 50 LIMIT 25");

        assertThat(MysqlSqlPreprocessor.preprocess("SELECT * FROM `my_table` LIMIT 100 OFFSET 200"))
                .isEqualTo("SELECT * FROM \"my_table\" OFFSET 200 LIMIT 100");
    }
}
