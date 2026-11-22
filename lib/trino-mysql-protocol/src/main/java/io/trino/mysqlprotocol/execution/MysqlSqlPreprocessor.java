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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Lexical SQL preprocessor that translates MySQL-specific syntax into Trino ANSI SQL:
 * <ul>
 *   <li>Rewrites backquoted identifiers ({@code `ident`}) into double-quoted identifiers ({@code "ident"}),
 *       unescaping {@code ``} to {@code `} and escaping embedded {@code "} as {@code ""}, while leaving
 *       single-quoted string literals, double-quoted identifiers, and comments intact.</li>
 *   <li>Rewrites MySQL backslash-escaped single quotes ({@code \'}) inside string literals to ANSI {@code ''}.</li>
 *   <li>Rewrites MySQL {@code #} line comments outside literals/identifiers into ANSI {@code --} comments.</li>
 *   <li>Strips trailing semicolons (which Trino's {@code /v1/statement} parser rejects).</li>
 *   <li>Rewrites MySQL {@code LIMIT <offset>, <count>} and {@code LIMIT <count> OFFSET <offset>} into
 *       Trino's {@code OFFSET <offset> LIMIT <count>}.</li>
 * </ul>
 */
public final class MysqlSqlPreprocessor
{
    private static final Pattern LIMIT_COMMA_PATTERN = Pattern.compile(
            "(?i)\\bLIMIT\\s+(\\d+)\\s*,\\s*(\\d+)\\s*$");
    private static final Pattern LIMIT_OFFSET_PATTERN = Pattern.compile(
            "(?i)\\bLIMIT\\s+(\\d+)\\s+OFFSET\\s+(\\d+)\\s*$");

    private MysqlSqlPreprocessor() {}

    public static String preprocess(String rawSql)
    {
        if (rawSql == null || rawSql.isEmpty()) {
            return "";
        }

        String translated = rewriteLexicalTokens(rawSql);
        translated = stripTrailingSemicolons(translated);
        translated = rewriteMySqlLimit(translated);
        return translated;
    }

    private static String rewriteLexicalTokens(String sql)
    {
        int len = sql.length();
        StringBuilder out = new StringBuilder(len + 16);
        int i = 0;

        while (i < len) {
            char c = sql.charAt(i);

            // 1. Single-quoted string literal: '...'
            if (c == '\'') {
                out.append('\'');
                i++;
                while (i < len) {
                    char sc = sql.charAt(i);
                    if (sc == '\\' && i + 1 < len) {
                        char next = sql.charAt(i + 1);
                        if (next == '\'') {
                            // Convert MySQL \' to ANSI ''
                            out.append("''");
                            i += 2;
                            continue;
                        }
                        if (next == '\\') {
                            out.append("\\\\");
                            i += 2;
                            continue;
                        }
                    }
                    if (sc == '\'') {
                        out.append('\'');
                        i++;
                        if (i < len && sql.charAt(i) == '\'') {
                            // ANSI '' escape
                            out.append('\'');
                            i++;
                            continue;
                        }
                        break;
                    }
                    out.append(sc);
                    i++;
                }
                continue;
            }

            // 2. Double-quoted identifier: "..."
            if (c == '"') {
                out.append('"');
                i++;
                while (i < len) {
                    char dc = sql.charAt(i);
                    out.append(dc);
                    i++;
                    if (dc == '"') {
                        if (i < len && sql.charAt(i) == '"') {
                            out.append('"');
                            i++;
                            continue;
                        }
                        break;
                    }
                }
                continue;
            }

            // 3. Backquoted identifier: `...` -> "..."
            if (c == '`') {
                out.append('"');
                i++;
                while (i < len) {
                    char bc = sql.charAt(i);
                    if (bc == '`') {
                        i++;
                        if (i < len && sql.charAt(i) == '`') {
                            // Escaped backtick `` inside backquoted identifier
                            out.append('`');
                            i++;
                            continue;
                        }
                        // Closing backtick
                        break;
                    }
                    if (bc == '"') {
                        // Escape embedded double quote for ANSI quoted identifier
                        out.append("\"\"");
                    }
                    else {
                        out.append(bc);
                    }
                    i++;
                }
                out.append('"');
                continue;
            }

            // 4. ANSI line comment: -- ...
            if (c == '-' && i + 1 < len && sql.charAt(i + 1) == '-') {
                out.append("--");
                i += 2;
                while (i < len) {
                    char lc = sql.charAt(i);
                    out.append(lc);
                    i++;
                    if (lc == '\n') {
                        break;
                    }
                }
                continue;
            }

            // 5. MySQL hash line comment: # ... -> -- ...
            if (c == '#') {
                out.append("-- ");
                i++;
                while (i < len) {
                    char hc = sql.charAt(i);
                    out.append(hc);
                    i++;
                    if (hc == '\n') {
                        break;
                    }
                }
                continue;
            }

            // 6. Block comment: /* ... */
            if (c == '/' && i + 1 < len && sql.charAt(i + 1) == '*') {
                out.append("/*");
                i += 2;
                while (i < len) {
                    if (sql.charAt(i) == '*' && i + 1 < len && sql.charAt(i + 1) == '/') {
                        out.append("*/");
                        i += 2;
                        break;
                    }
                    out.append(sql.charAt(i));
                    i++;
                }
                continue;
            }

            out.append(c);
            i++;
        }

        return out.toString();
    }

    private static String stripTrailingSemicolons(String sql)
    {
        int end = sql.length();
        while (end > 0) {
            char c = sql.charAt(end - 1);
            if (Character.isWhitespace(c) || c == ';') {
                end--;
            }
            else {
                break;
            }
        }
        return end == sql.length() ? sql : sql.substring(0, end);
    }

    private static String rewriteMySqlLimit(String sql)
    {
        Matcher commaMatcher = LIMIT_COMMA_PATTERN.matcher(sql);
        if (commaMatcher.find()) {
            String offset = commaMatcher.group(1);
            String count = commaMatcher.group(2);
            return sql.substring(0, commaMatcher.start()) + "OFFSET " + offset + " LIMIT " + count;
        }

        Matcher offsetMatcher = LIMIT_OFFSET_PATTERN.matcher(sql);
        if (offsetMatcher.find()) {
            String count = offsetMatcher.group(1);
            String offset = offsetMatcher.group(2);
            return sql.substring(0, offsetMatcher.start()) + "OFFSET " + offset + " LIMIT " + count;
        }

        return sql;
    }
}
