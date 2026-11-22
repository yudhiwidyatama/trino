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

import com.google.inject.Inject;
import io.trino.client.ClientSession;
import io.trino.mysqlprotocol.MysqlProtocolConfig;
import io.trino.mysqlprotocol.execution.MysqlSystemVariableHandler.InterceptedResult;
import io.trino.mysqlprotocol.execution.TrinoStatementExecutor.InternalQueryResult;
import io.trino.mysqlprotocol.session.MysqlSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

/**
 * Translates MySQL GUI / client metadata queries (such as those emitted by Sequel Pro, DBeaver,
 * TablePlus, Navicat, and MySQL Workbench) into Trino metadata queries and formats the responses
 * with the exact column names and DDL syntax expected by MySQL clients:
 * <ul>
 *   <li>{@code SHOW DATABASES}</li>
 *   <li>{@code SHOW [FULL] TABLES [FROM db] [LIKE '...']}</li>
 *   <li>{@code SHOW TABLE STATUS [FROM db] [LIKE '...']}</li>
 *   <li>{@code SHOW [FULL] COLUMNS/FIELDS FROM tbl [FROM db]}</li>
 *   <li>{@code SHOW CREATE TABLE tbl}</li>
 * </ul>
 */
public class MysqlMetadataQueryHandler
{
    private static final Pattern LEADING_BLOCK_COMMENT = Pattern.compile("^\\s*(/\\*.*?\\*/\\s*)+", Pattern.DOTALL);

    private static final Pattern SHOW_DATABASES = Pattern.compile(
            "^SHOW\\s+(?:DATABASES|SCHEMAS)(?:\\s+(?:FROM|IN)\\s+([A-Za-z0-9_.\"`]+))?(?:\\s+LIKE\\s+'([^']*)'|\\s+WHERE\\s+.*)?\\s*$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private static final Pattern SHOW_TABLES = Pattern.compile(
            "^SHOW\\s+(FULL\\s+)?TABLES(?:\\s+(?:FROM|IN)\\s+([A-Za-z0-9_.\"`]+))?(?:\\s+LIKE\\s+'([^']*)'|\\s+WHERE\\s+.*)?\\s*$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private static final Pattern SHOW_TABLE_STATUS = Pattern.compile(
            "^SHOW\\s+TABLE\\s+STATUS(?:\\s+(?:FROM|IN)\\s+([A-Za-z0-9_.\"`]+))?(?:\\s+LIKE\\s+'([^']*)'|\\s+WHERE\\s+[\"`']?Name[\"`']?\\s*(?:=|LIKE)\\s*'([^']*)'|\\s+WHERE\\s+.*)?\\s*$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private static final Pattern SHOW_COLUMNS = Pattern.compile(
            "^SHOW\\s+(FULL\\s+)?(?:COLUMNS|FIELDS)\\s+(?:FROM|IN)\\s+([A-Za-z0-9_.\"`]+)(?:\\s+(?:FROM|IN)\\s+([A-Za-z0-9_.\"`]+))?(?:\\s+LIKE\\s+'([^']*)'|\\s+WHERE\\s+.*)?\\s*$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private static final Pattern SHOW_CREATE_TABLE = Pattern.compile(
            "^SHOW\\s+CREATE\\s+TABLE\\s+([A-Za-z0-9_.\"`]+)\\s*$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private static final List<String> TABLE_STATUS_COLUMNS = List.of(
            "Name",
            "Engine",
            "Version",
            "Row_format",
            "Rows",
            "Avg_row_length",
            "Data_length",
            "Max_data_length",
            "Index_length",
            "Data_free",
            "Auto_increment",
            "Create_time",
            "Update_time",
            "Check_time",
            "Collation",
            "Checksum",
            "Create_options",
            "Comment");

    private static final List<String> FULL_COLUMNS_HEADERS = List.of(
            "Field",
            "Type",
            "Collation",
            "Null",
            "Key",
            "Default",
            "Extra",
            "Privileges",
            "Comment");

    private static final List<String> SIMPLE_COLUMNS_HEADERS = List.of(
            "Field",
            "Type",
            "Null",
            "Key",
            "Default",
            "Extra");

    private final MysqlProtocolConfig config;
    private final TrinoStatementExecutor statementExecutor;

    @Inject
    public MysqlMetadataQueryHandler(MysqlProtocolConfig config, TrinoStatementExecutor statementExecutor)
    {
        this.config = requireNonNull(config, "config is null");
        this.statementExecutor = requireNonNull(statementExecutor, "statementExecutor is null");
    }

    public Optional<InterceptedResult> tryHandle(String preprocessedSql, MysqlSession session)
    {
        if (!config.isCompatInterceptorEnabled()) {
            return Optional.empty();
        }

        String sql = stripLeadingComments(preprocessedSql).trim();
        if (sql.isEmpty()) {
            return Optional.empty();
        }

        Matcher showDbMatcher = SHOW_DATABASES.matcher(sql);
        if (showDbMatcher.matches()) {
            // Only intercept SHOW DATABASES, or SHOW SCHEMAS when no catalog is set in the session
            boolean isShowDatabases = sql.toUpperCase(Locale.ROOT).startsWith("SHOW DATABASES");
            if (isShowDatabases || (showDbMatcher.group(1) == null && session.clientSession().getCatalog().isEmpty())) {
                return Optional.of(handleShowDatabases(
                        Optional.ofNullable(showDbMatcher.group(1)),
                        Optional.ofNullable(showDbMatcher.group(2)),
                        isShowDatabases ? "Database" : "Schema",
                        session));
            }
        }

        Matcher showTablesMatcher = SHOW_TABLES.matcher(sql);
        if (showTablesMatcher.matches()) {
            boolean isFull = showTablesMatcher.group(1) != null;
            Optional<String> fromClause = Optional.ofNullable(showTablesMatcher.group(2));
            Optional<String> likePattern = Optional.ofNullable(showTablesMatcher.group(3));
            return Optional.of(handleShowTables(isFull, fromClause, likePattern, session));
        }

        Matcher showTableStatusMatcher = SHOW_TABLE_STATUS.matcher(sql);
        if (showTableStatusMatcher.matches()) {
            Optional<String> fromClause = Optional.ofNullable(showTableStatusMatcher.group(1));
            Optional<String> likePattern = Optional.ofNullable(showTableStatusMatcher.group(2))
                    .or(() -> Optional.ofNullable(showTableStatusMatcher.group(3)));
            return Optional.of(handleShowTableStatus(fromClause, likePattern, session));
        }

        Matcher showColumnsMatcher = SHOW_COLUMNS.matcher(sql);
        if (showColumnsMatcher.matches()) {
            boolean isFull = showColumnsMatcher.group(1) != null;
            String tableSpec = showColumnsMatcher.group(2);
            Optional<String> fromDb = Optional.ofNullable(showColumnsMatcher.group(3));
            Optional<String> likePattern = Optional.ofNullable(showColumnsMatcher.group(4));
            return Optional.of(handleShowColumns(isFull, tableSpec, fromDb, likePattern, session));
        }

        Matcher showCreateTableMatcher = SHOW_CREATE_TABLE.matcher(sql);
        if (showCreateTableMatcher.matches()) {
            String tableSpec = showCreateTableMatcher.group(1);
            return Optional.of(handleShowCreateTable(tableSpec, session));
        }

        return Optional.empty();
    }

    private InterceptedResult handleShowDatabases(
            Optional<String> fromCatalog,
            Optional<String> likePattern,
            String columnHeader,
            MysqlSession session)
    {
        Optional<String> catalog = fromCatalog.map(MysqlMetadataQueryHandler::unquoteIdentifier)
                .or(() -> session.clientSession().getCatalog());

        String likeClause = likePattern.map(p -> " LIKE '" + p.replace("'", "''") + "'").orElse("");
        if (catalog.isPresent()) {
            String cat = catalog.get();
            InternalQueryResult res = statementExecutor.executeInternalQuery(
                    session,
                    "SHOW SCHEMAS FROM " + quoteIdentifier(cat) + likeClause);

            List<String> schemaNames = new ArrayList<>(res.rows().size());
            List<List<String>> rows = new ArrayList<>(res.rows().size());
            for (List<Object> row : res.rows()) {
                if (!row.isEmpty() && row.get(0) != null) {
                    String schemaName = String.valueOf(row.get(0));
                    schemaNames.add(schemaName);
                    rows.add(List.of(schemaName));
                }
            }

            // If session has catalog set but no schema selected yet, auto-select a sensible default schema
            if (session.clientSession().getSchema().isEmpty() && !schemaNames.isEmpty()) {
                String pickedSchema = pickDefaultSchema(schemaNames);
                session.setClientSession(ClientSession.builder(session.clientSession())
                        .catalog(cat)
                        .schema(pickedSchema)
                        .build());
            }

            return InterceptedResult.resultSet(List.of(columnHeader), rows);
        }

        InternalQueryResult res = statementExecutor.executeInternalQuery(session, "SHOW CATALOGS" + likeClause);
        List<List<String>> rows = new ArrayList<>(res.rows().size());
        for (List<Object> row : res.rows()) {
            if (!row.isEmpty() && row.get(0) != null) {
                rows.add(List.of(String.valueOf(row.get(0))));
            }
        }
        return InterceptedResult.resultSet(List.of(columnHeader), rows);
    }

    private InterceptedResult handleShowTables(
            boolean isFull,
            Optional<String> fromClause,
            Optional<String> likePattern,
            MysqlSession session)
    {
        ResolvedSchema target = resolveTargetSchema(fromClause, session);
        String colName = "Tables_in_" + target.displayLabel();
        List<String> headers = isFull ? List.of(colName, "Table_type") : List.of(colName);

        if (target.qualifiedTrinoSchema().isEmpty()) {
            return InterceptedResult.resultSet(headers, List.of());
        }

        String likeClause = likePattern.map(p -> " LIKE '" + p.replace("'", "''") + "'").orElse("");
        InternalQueryResult res = statementExecutor.executeInternalQuery(
                session,
                "SHOW TABLES FROM " + target.qualifiedTrinoSchema().get() + likeClause);

        List<List<String>> rows = new ArrayList<>(res.rows().size());
        for (List<Object> row : res.rows()) {
            if (!row.isEmpty() && row.get(0) != null) {
                String tableName = String.valueOf(row.get(0));
                if (isFull) {
                    rows.add(List.of(tableName, "BASE TABLE"));
                }
                else {
                    rows.add(List.of(tableName));
                }
            }
        }
        return InterceptedResult.resultSet(headers, rows);
    }

    private InterceptedResult handleShowTableStatus(
            Optional<String> fromClause,
            Optional<String> likePattern,
            MysqlSession session)
    {
        ResolvedSchema target = resolveTargetSchema(fromClause, session);
        if (target.qualifiedTrinoSchema().isEmpty()) {
            return InterceptedResult.resultSet(TABLE_STATUS_COLUMNS, List.of());
        }

        String likeClause = likePattern.map(p -> " LIKE '" + p.replace("'", "''") + "'").orElse("");
        InternalQueryResult res = statementExecutor.executeInternalQuery(
                session,
                "SHOW TABLES FROM " + target.qualifiedTrinoSchema().get() + likeClause);

        List<List<String>> rows = new ArrayList<>(res.rows().size());
        for (List<Object> row : res.rows()) {
            if (!row.isEmpty() && row.get(0) != null) {
                String tableName = String.valueOf(row.get(0));
                rows.add(Arrays.asList(
                        tableName,
                        "InnoDB",
                        "10",
                        "Dynamic",
                        "0",
                        "0",
                        "0",
                        "0",
                        "0",
                        "0",
                        null,
                        null,
                        null,
                        null,
                        "utf8mb4_0900_ai_ci",
                        null,
                        "",
                        ""));
            }
        }
        return InterceptedResult.resultSet(TABLE_STATUS_COLUMNS, rows);
    }

    private InterceptedResult handleShowColumns(
            boolean isFull,
            String tableSpec,
            Optional<String> fromDb,
            Optional<String> likePattern,
            MysqlSession session)
    {
        String qualifiedTable = qualifyTableReference(tableSpec, fromDb);
        InternalQueryResult res = statementExecutor.executeInternalQuery(session, "DESCRIBE " + qualifiedTable);

        Pattern filterRegex = likePattern.map(MysqlMetadataQueryHandler::sqlLikeToRegex).orElse(null);
        List<List<String>> rows = new ArrayList<>(res.rows().size());

        for (List<Object> row : res.rows()) {
            String colName = row.size() > 0 && row.get(0) != null ? String.valueOf(row.get(0)) : "";
            if (filterRegex != null && !filterRegex.matcher(colName).matches()) {
                continue;
            }
            String trinoType = row.size() > 1 && row.get(1) != null ? String.valueOf(row.get(1)) : "varchar";
            String extra = row.size() > 2 && row.get(2) != null ? String.valueOf(row.get(2)) : "";
            String comment = row.size() > 3 && row.get(3) != null ? String.valueOf(row.get(3)) : "";
            String mysqlType = toMysqlColumnType(trinoType);

            if (isFull) {
                String collation = isStringType(trinoType) ? "utf8mb4_0900_ai_ci" : null;
                rows.add(Arrays.asList(
                        colName,
                        mysqlType,
                        collation,
                        "YES",
                        "",
                        null,
                        extra,
                        "select,insert,update,references",
                        comment));
            }
            else {
                rows.add(Arrays.asList(
                        colName,
                        mysqlType,
                        "YES",
                        "",
                        null,
                        extra));
            }
        }

        return InterceptedResult.resultSet(isFull ? FULL_COLUMNS_HEADERS : SIMPLE_COLUMNS_HEADERS, rows);
    }

    private InterceptedResult handleShowCreateTable(String tableSpec, MysqlSession session)
    {
        List<String> parts = splitQualifiedIdentifier(tableSpec);
        String unqualifiedTable = parts.get(parts.size() - 1);
        String qualifiedTable = qualifyTableReference(tableSpec, Optional.empty());

        InternalQueryResult res = statementExecutor.executeInternalQuery(session, "DESCRIBE " + qualifiedTable);

        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE `").append(escapeBacktick(unqualifiedTable)).append("` (\n");

        List<List<Object>> describeRows = res.rows();
        for (int i = 0; i < describeRows.size(); i++) {
            List<Object> row = describeRows.get(i);
            String colName = row.size() > 0 && row.get(0) != null ? String.valueOf(row.get(0)) : "col_" + i;
            String trinoType = row.size() > 1 && row.get(1) != null ? String.valueOf(row.get(1)) : "varchar";
            String mysqlType = toMysqlColumnType(trinoType);

            ddl.append("  `").append(escapeBacktick(colName)).append("` ").append(mysqlType).append(" DEFAULT NULL");
            if (i < describeRows.size() - 1) {
                ddl.append(',');
            }
            ddl.append('\n');
        }
        ddl.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci");

        return InterceptedResult.resultSet(
                List.of("Table", "Create Table"),
                List.of(List.of(unqualifiedTable, ddl.toString())));
    }

    private ResolvedSchema resolveTargetSchema(Optional<String> fromClause, MysqlSession session)
    {
        ClientSession cs = session.clientSession();
        if (fromClause.isPresent()) {
            List<String> parts = splitQualifiedIdentifier(fromClause.get());
            if (parts.size() >= 2) {
                String cat = parts.get(0);
                String sch = parts.get(1);
                return new ResolvedSchema(
                        Optional.of(quoteIdentifier(cat) + "." + quoteIdentifier(sch)),
                        sch);
            }
            String single = parts.get(0);
            if (cs.getCatalog().isPresent()) {
                return new ResolvedSchema(
                        Optional.of(quoteIdentifier(cs.getCatalog().get()) + "." + quoteIdentifier(single)),
                        single);
            }
            return new ResolvedSchema(Optional.of(quoteIdentifier(single)), single);
        }

        if (cs.getCatalog().isPresent() && cs.getSchema().isPresent()) {
            return new ResolvedSchema(
                    Optional.of(quoteIdentifier(cs.getCatalog().get()) + "." + quoteIdentifier(cs.getSchema().get())),
                    cs.getSchema().get());
        }
        if (cs.getSchema().isPresent()) {
            return new ResolvedSchema(
                    Optional.of(quoteIdentifier(cs.getSchema().get())),
                    cs.getSchema().get());
        }
        if (cs.getCatalog().isPresent()) {
            String cat = cs.getCatalog().get();
            // Try to auto-select a default schema for this catalog
            try {
                InternalQueryResult schemasRes = statementExecutor.executeInternalQuery(
                        session,
                        "SHOW SCHEMAS FROM " + quoteIdentifier(cat));
                List<String> schemas = new ArrayList<>();
                for (List<Object> row : schemasRes.rows()) {
                    if (!row.isEmpty() && row.get(0) != null) {
                        schemas.add(String.valueOf(row.get(0)));
                    }
                }
                if (!schemas.isEmpty()) {
                    String picked = pickDefaultSchema(schemas);
                    session.setClientSession(ClientSession.builder(cs).catalog(cat).schema(picked).build());
                    return new ResolvedSchema(
                            Optional.of(quoteIdentifier(cat) + "." + quoteIdentifier(picked)),
                            picked);
                }
            }
            catch (Exception ignored) {
                // Fall through to empty schema
            }
            return new ResolvedSchema(Optional.empty(), cat);
        }
        return new ResolvedSchema(Optional.empty(), "trino");
    }

    public static String pickDefaultSchema(List<String> schemas)
    {
        for (String s : schemas) {
            if ("default".equalsIgnoreCase(s)) {
                return s;
            }
        }
        for (String s : schemas) {
            if (!"information_schema".equalsIgnoreCase(s)) {
                return s;
            }
        }
        return schemas.get(0);
    }

    public static String toMysqlColumnType(String trinoTypeRaw)
    {
        if (trinoTypeRaw == null || trinoTypeRaw.isBlank()) {
            return "text";
        }
        String t = trinoTypeRaw.trim().toLowerCase(Locale.ROOT);
        if (t.equals("boolean") || t.equals("bool")) {
            return "tinyint(1)";
        }
        if (t.equals("tinyint")) {
            return "tinyint(4)";
        }
        if (t.equals("smallint")) {
            return "smallint(6)";
        }
        if (t.equals("integer") || t.equals("int")) {
            return "int(11)";
        }
        if (t.equals("bigint")) {
            return "bigint(20)";
        }
        if (t.equals("real") || t.equals("float")) {
            return "float";
        }
        if (t.equals("double") || t.equals("double precision")) {
            return "double";
        }
        if (t.startsWith("decimal(") || t.startsWith("numeric(")) {
            int open = t.indexOf('(');
            int close = t.indexOf(')', open);
            if (open > 0 && close > open) {
                String args = t.substring(open + 1, close).replaceAll("\\s+", "");
                return "decimal(" + args + ")";
            }
            return "decimal(38,0)";
        }
        if (t.equals("decimal") || t.equals("numeric")) {
            return "decimal(38,0)";
        }
        if (t.equals("date")) {
            return "date";
        }
        if (t.startsWith("timestamp")) {
            return "datetime";
        }
        if (t.startsWith("time")) {
            return "time";
        }
        if (t.startsWith("varchar(") || t.startsWith("character varying(")) {
            int open = t.indexOf('(');
            int close = t.indexOf(')', open);
            if (open > 0 && close > open) {
                String lenStr = t.substring(open + 1, close).trim();
                try {
                    int len = Integer.parseInt(lenStr);
                    if (len > 0 && len <= 16383) {
                        return "varchar(" + len + ")";
                    }
                }
                catch (NumberFormatException ignored) {
                }
            }
            return "text";
        }
        if (t.equals("varchar") || t.equals("character varying")) {
            return "text";
        }
        if (t.startsWith("char(") || t.startsWith("character(")) {
            int open = t.indexOf('(');
            int close = t.indexOf(')', open);
            if (open > 0 && close > open) {
                String lenStr = t.substring(open + 1, close).trim();
                return "char(" + lenStr + ")";
            }
            return "char(1)";
        }
        if (t.equals("char") || t.equals("character")) {
            return "char(1)";
        }
        if (t.startsWith("varbinary")) {
            return "blob";
        }
        if (t.equals("json")) {
            return "json";
        }
        if (t.equals("uuid")) {
            return "char(36)";
        }
        return "text";
    }

    private static boolean isStringType(String trinoTypeRaw)
    {
        if (trinoTypeRaw == null) {
            return false;
        }
        String t = trinoTypeRaw.trim().toLowerCase(Locale.ROOT);
        return t.startsWith("varchar")
                || t.startsWith("char")
                || t.equals("json")
                || t.equals("uuid")
                || t.equals("ipaddress")
                || t.startsWith("array")
                || t.startsWith("map")
                || t.startsWith("row");
    }

    public static String qualifyTableReference(String tableSpec, Optional<String> fromDb)
    {
        List<String> tableParts = splitQualifiedIdentifier(tableSpec);
        List<String> combined = new ArrayList<>();
        if (fromDb.isPresent() && tableParts.size() == 1) {
            combined.addAll(splitQualifiedIdentifier(fromDb.get()));
        }
        combined.addAll(tableParts);

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < combined.size(); i++) {
            if (i > 0) {
                sb.append('.');
            }
            sb.append(quoteIdentifier(combined.get(i)));
        }
        return sb.toString();
    }

    public static List<String> splitQualifiedIdentifier(String raw)
    {
        String trimmed = raw.trim();
        List<String> parts = new ArrayList<>(3);
        StringBuilder current = new StringBuilder();
        char quote = 0;

        for (int i = 0; i < trimmed.length(); i++) {
            char c = trimmed.charAt(i);
            if (quote != 0) {
                if (c == quote) {
                    if (i + 1 < trimmed.length() && trimmed.charAt(i + 1) == quote) {
                        current.append(quote);
                        i++;
                    }
                    else {
                        quote = 0;
                    }
                }
                else {
                    current.append(c);
                }
            }
            else if (c == '"' || c == '`') {
                quote = c;
            }
            else if (c == '.') {
                parts.add(current.toString().trim());
                current.setLength(0);
            }
            else {
                current.append(c);
            }
        }
        if (current.length() > 0 || !parts.isEmpty()) {
            parts.add(current.toString().trim());
        }
        return parts.isEmpty() ? List.of(unquoteIdentifier(trimmed)) : parts;
    }

    public static String unquoteIdentifier(String identifier)
    {
        String s = identifier.trim();
        if (s.length() >= 2) {
            if (s.startsWith("\"") && s.endsWith("\"")) {
                return s.substring(1, s.length() - 1).replace("\"\"", "\"");
            }
            if (s.startsWith("`") && s.endsWith("`")) {
                return s.substring(1, s.length() - 1).replace("``", "`");
            }
        }
        return s;
    }

    public static String quoteIdentifier(String identifier)
    {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private static String escapeBacktick(String identifier)
    {
        return identifier.replace("`", "``");
    }

    private static String stripLeadingComments(String sql)
    {
        return LEADING_BLOCK_COMMENT.matcher(sql).replaceFirst("");
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

    private record ResolvedSchema(Optional<String> qualifiedTrinoSchema, String displayLabel) {}
}
