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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.client.ClientSession;
import io.trino.mysqlprotocol.MysqlProtocolConfig;
import io.trino.mysqlprotocol.session.MysqlSession;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

/**
 * Intercepts MySQL client/driver initialization probes (such as Connector/J's {@code SELECT @@...},
 * {@code SET NAMES}, {@code SHOW VARIABLES}, and {@code SELECT DATABASE()}) so standard MySQL drivers
 * can connect to Trino seamlessly without SQL syntax errors.
 */
public class MysqlSystemVariableHandler
{
    private static final Pattern LEADING_BLOCK_COMMENT = Pattern.compile("^\\s*(/\\*.*?\\*/\\s*)+", Pattern.DOTALL);
    private static final Pattern SELECT_SYSVARS = Pattern.compile("^SELECT\\s+(.+?)(?:\\s+LIMIT\\s+\\d+)?\\s*;?$", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern SYSVAR_ITEM = Pattern.compile(
            "^@@(?:(?:SESSION|GLOBAL|LOCAL)\\.)?([A-Za-z0-9_]+)(?:\\s+(?:AS\\s+)?`?([A-Za-z0-9_]+)`?)?$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern SET_TIME_ZONE = Pattern.compile(
            "^SET\\s+(?:(?:SESSION|LOCAL|@@SESSION\\.|@@)\\s*)?time_zone\\s*=\\s*['\"]?([^'\";]+)['\"]?\\s*;?$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern MYSQL_COMPAT_SET = Pattern.compile(
            "^SET\\s+(?:NAMES\\b|CHARACTER\\s+SET\\b|(?:SESSION\\s+|LOCAL\\s+|@@SESSION\\.|@@)?(?:character_set_results|character_set_client|character_set_connection|collation_connection|sql_mode|autocommit|net_write_timeout|net_read_timeout|wait_timeout|interactive_timeout|transaction_isolation|tx_isolation|sql_select_limit|max_execution_time|SESSION\\s+TRANSACTION)\\b).*",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern SHOW_VARIABLES = Pattern.compile(
            "^SHOW\\s+(?:(?:SESSION|GLOBAL)\\s+)?VARIABLES(?:\\s+LIKE\\s+'([^']*)')?\\s*;?$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern SHOW_COLLATION = Pattern.compile("^SHOW\\s+COLLATION\\b.*", Pattern.CASE_INSENSITIVE);
    private static final Pattern SELECT_DATABASE = Pattern.compile("^SELECT\\s+DATABASE\\(\\)\\s*;?$", Pattern.CASE_INSENSITIVE);
    private static final Pattern SELECT_VERSION = Pattern.compile("^SELECT\\s+VERSION\\(\\)\\s*;?$", Pattern.CASE_INSENSITIVE);

    private final MysqlProtocolConfig config;

    @Inject
    public MysqlSystemVariableHandler(MysqlProtocolConfig config)
    {
        this.config = requireNonNull(config, "config is null");
    }

    public Optional<InterceptedResult> tryHandle(String rawSql, MysqlSession session)
    {
        if (!config.isCompatInterceptorEnabled()) {
            return Optional.empty();
        }
        String sql = stripLeadingComments(rawSql).trim();
        if (sql.isEmpty()) {
            return Optional.of(InterceptedResult.ok());
        }

        if (SELECT_DATABASE.matcher(sql).matches()) {
            String db = formatDatabase(session.clientSession());
            return Optional.of(InterceptedResult.resultSet(List.of("DATABASE()"), List.of(singletonRow(db))));
        }

        if (SELECT_VERSION.matcher(sql).matches()) {
            return Optional.of(InterceptedResult.resultSet(List.of("VERSION()"), List.of(List.of(config.getServerVersion()))));
        }

        Matcher tzMatcher = SET_TIME_ZONE.matcher(sql);
        if (tzMatcher.matches()) {
            String tz = tzMatcher.group(1).trim();
            ZoneId zoneId = "SYSTEM".equalsIgnoreCase(tz) ? ZoneId.systemDefault() : ZoneId.of(tz);
            session.setClientSession(ClientSession.builder(session.clientSession()).timeZone(zoneId).build());
            return Optional.of(InterceptedResult.ok());
        }

        if (MYSQL_COMPAT_SET.matcher(sql).matches()) {
            return Optional.of(InterceptedResult.ok());
        }

        Matcher showVarsMatcher = SHOW_VARIABLES.matcher(sql);
        if (showVarsMatcher.matches()) {
            String likePattern = showVarsMatcher.group(1);
            return Optional.of(handleShowVariables(likePattern, session));
        }

        if (SHOW_COLLATION.matcher(sql).matches()) {
            return Optional.of(InterceptedResult.resultSet(
                    List.of("Collation", "Charset", "Id", "Default", "Compiled", "Sortlen", "Pad_attribute"),
                    List.of(List.of("utf8mb4_0900_ai_ci", "utf8mb4", "255", "Yes", "Yes", "0", "NO PAD"),
                            List.of("utf8mb4_general_ci", "utf8mb4", "45", "", "Yes", "1", "PAD SPACE"))));
        }

        Matcher selectMatcher = SELECT_SYSVARS.matcher(sql);
        if (selectMatcher.matches()) {
            String selectList = selectMatcher.group(1);
            if (selectList.contains("@@") && !selectList.toUpperCase(Locale.ROOT).contains(" FROM ")) {
                return tryResolveSystemVariableSelect(selectList, session);
            }
        }

        return Optional.empty();
    }

    private Optional<InterceptedResult> tryResolveSystemVariableSelect(String selectList, MysqlSession session)
    {
        String[] parts = selectList.split(",");
        List<String> columnNames = new ArrayList<>(parts.length);
        List<String> values = new ArrayList<>(parts.length);

        for (String part : parts) {
            Matcher item = SYSVAR_ITEM.matcher(part.trim());
            if (!item.matches()) {
                return Optional.empty();
            }
            String varName = item.group(1);
            String alias = item.group(2) != null ? item.group(2) : part.trim();
            columnNames.add(alias);
            values.add(resolveVariable(varName, session));
        }

        return Optional.of(InterceptedResult.resultSet(columnNames, List.of(values)));
    }

    private InterceptedResult handleShowVariables(String likePattern, MysqlSession session)
    {
        List<String> knownVars = ImmutableList.of(
                "auto_increment_increment",
                "character_set_client",
                "character_set_connection",
                "character_set_results",
                "character_set_server",
                "collation_connection",
                "collation_server",
                "init_connect",
                "interactive_timeout",
                "license",
                "lower_case_table_names",
                "max_allowed_packet",
                "net_buffer_length",
                "net_write_timeout",
                "performance_schema",
                "query_cache_size",
                "query_cache_type",
                "sql_mode",
                "system_time_zone",
                "time_zone",
                "transaction_isolation",
                "tx_isolation",
                "version",
                "version_comment",
                "wait_timeout");

        Pattern regex = likePattern == null ? null : sqlLikeToRegex(likePattern);
        List<List<String>> rows = new ArrayList<>();
        for (String name : knownVars) {
            if (regex == null || regex.matcher(name).matches()) {
                rows.add(List.of(name, resolveVariable(name, session)));
            }
        }
        return InterceptedResult.resultSet(List.of("Variable_name", "Value"), rows);
    }

    private String resolveVariable(String varName, MysqlSession session)
    {
        return switch (varName.toLowerCase(Locale.ROOT)) {
            case "auto_increment_increment" -> "1";
            case "character_set_client", "character_set_connection", "character_set_results", "character_set_server" -> "utf8mb4";
            case "collation_connection", "collation_server" -> "utf8mb4_0900_ai_ci";
            case "max_allowed_packet" -> String.valueOf(config.getMaxAllowedPacket().toBytes());
            case "net_buffer_length" -> "16384";
            case "net_write_timeout", "net_read_timeout" -> "60";
            case "interactive_timeout", "wait_timeout" -> String.valueOf((long) config.getIdleTimeout().getValue(java.util.concurrent.TimeUnit.SECONDS));
            case "system_time_zone" -> ZoneId.systemDefault().getId();
            case "time_zone" -> session.clientSession().getTimeZone().getId();
            case "transaction_isolation", "tx_isolation" -> "READ-COMMITTED";
            case "transaction_read_only", "tx_read_only", "lower_case_table_names", "performance_schema", "query_cache_size" -> "0";
            case "query_cache_type" -> "OFF";
            case "sql_mode", "init_connect" -> "";
            case "license" -> "Apache-2.0";
            case "version" -> config.getServerVersion();
            case "version_comment" -> "Trino MySQL Protocol";
            case "autocommit" -> "1";
            default -> "";
        };
    }

    private static String stripLeadingComments(String sql)
    {
        return LEADING_BLOCK_COMMENT.matcher(sql).replaceFirst("");
    }

    private static String formatDatabase(ClientSession cs)
    {
        if (cs.getCatalog().isPresent() && cs.getSchema().isPresent()) {
            return cs.getCatalog().get() + "." + cs.getSchema().get();
        }
        if (cs.getSchema().isPresent()) {
            return cs.getSchema().get();
        }
        return cs.getCatalog().orElse(null);
    }

    private static List<String> singletonRow(String value)
    {
        List<String> row = new ArrayList<>(1);
        row.add(value);
        return row;
    }

    private static Pattern sqlLikeToRegex(String like)
    {
        StringBuilder sb = new StringBuilder("^");
        for (int i = 0; i < like.length(); i++) {
            char c = like.charAt(i);
            if (c == '%') {
                sb.append(".*");
            }
            else if (c == '_') {
                sb.append('.');
            }
            else {
                sb.append(Pattern.quote(String.valueOf(c)));
            }
        }
        sb.append('$');
        return Pattern.compile(sb.toString(), Pattern.CASE_INSENSITIVE);
    }

    public record InterceptedResult(boolean isResultSet, List<String> columnNames, List<List<String>> rows)
    {
        public static InterceptedResult ok()
        {
            return new InterceptedResult(false, List.of(), List.of());
        }

        public static InterceptedResult resultSet(List<String> columnNames, List<List<String>> rows)
        {
            return new InterceptedResult(true, ImmutableList.copyOf(columnNames), rows);
        }
    }
}
