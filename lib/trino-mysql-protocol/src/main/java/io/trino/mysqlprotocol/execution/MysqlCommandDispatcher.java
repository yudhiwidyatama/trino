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
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.trino.client.ClientSession;
import io.trino.mysqlprotocol.MysqlProtocolConfig;
import io.trino.mysqlprotocol.execution.TrinoStatementExecutor.InternalQueryResult;
import io.trino.mysqlprotocol.execution.TrinoStatementExecutor.MysqlQueryException;
import io.trino.mysqlprotocol.metrics.MysqlProtocolStats;
import io.trino.mysqlprotocol.protocol.MysqlCommand;
import io.trino.mysqlprotocol.protocol.MysqlPacketBuffer;
import io.trino.mysqlprotocol.protocol.packet.EofPacket;
import io.trino.mysqlprotocol.protocol.packet.ErrPacket;
import io.trino.mysqlprotocol.protocol.packet.OkPacket;
import io.trino.mysqlprotocol.protocol.packet.ResultSetEndPacket;
import io.trino.mysqlprotocol.server.MysqlSessionRegistry;
import io.trino.mysqlprotocol.session.MysqlSession;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.mysqlprotocol.execution.MysqlMetadataQueryHandler.quoteIdentifier;
import static io.trino.mysqlprotocol.execution.TrinoStatementExecutor.writePacket;
import static io.trino.mysqlprotocol.execution.TrinoStatementExecutor.writePacketAndFlush;
import static java.util.Objects.requireNonNull;

/**
 * Dispatches incoming MySQL command packets (COM_*) to local handlers or Trino REST execution.
 */
public class MysqlCommandDispatcher
{
    private static final Pattern KILL_PATTERN = Pattern.compile(
            "^KILL\\s+(?:(QUERY|CONNECTION)\\s+)?(\\d+)\\s*;?$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern USE_SINGLE_IDENTIFIER_PATTERN = Pattern.compile(
            "^USE\\s+([A-Za-z0-9_]+|\"(?:[^\"]|\"\")+\")\\s*$",
            Pattern.CASE_INSENSITIVE);

    private final TrinoStatementExecutor statementExecutor;
    private final MysqlSystemVariableHandler systemVariableHandler;
    private final MysqlMetadataQueryHandler metadataQueryHandler;
    private final MysqlSessionRegistry sessionRegistry;
    private final MysqlProtocolConfig config;
    private final MysqlProtocolStats stats;

    public MysqlCommandDispatcher(
            TrinoStatementExecutor statementExecutor,
            MysqlSystemVariableHandler systemVariableHandler,
            MysqlSessionRegistry sessionRegistry,
            MysqlProtocolConfig config,
            MysqlProtocolStats stats)
    {
        this(
                statementExecutor,
                systemVariableHandler,
                new MysqlMetadataQueryHandler(config, statementExecutor),
                sessionRegistry,
                config,
                stats);
    }

    @Inject
    public MysqlCommandDispatcher(
            TrinoStatementExecutor statementExecutor,
            MysqlSystemVariableHandler systemVariableHandler,
            MysqlMetadataQueryHandler metadataQueryHandler,
            MysqlSessionRegistry sessionRegistry,
            MysqlProtocolConfig config,
            MysqlProtocolStats stats)
    {
        this.statementExecutor = requireNonNull(statementExecutor, "statementExecutor is null");
        this.systemVariableHandler = requireNonNull(systemVariableHandler, "systemVariableHandler is null");
        this.metadataQueryHandler = requireNonNull(metadataQueryHandler, "metadataQueryHandler is null");
        this.sessionRegistry = requireNonNull(sessionRegistry, "sessionRegistry is null");
        this.config = requireNonNull(config, "config is null");
        this.stats = requireNonNull(stats, "stats is null");
    }

    /**
     * @return {@code true} if the connection should remain open, {@code false} if it should be closed (COM_QUIT).
     */
    public boolean dispatch(ChannelHandlerContext ctx, MysqlSession session, ByteBuf payload)
    {
        if (!payload.isReadable()) {
            return false;
        }

        int commandCode = MysqlPacketBuffer.readInt1(payload);
        Optional<MysqlCommand> command = MysqlCommand.fromCode(commandCode);
        if (command.isEmpty()) {
            writePacketAndFlush(ctx, new ErrPacket(1047, "08S01", "Unknown command: " + commandCode));
            return true;
        }

        switch (command.get()) {
            case COM_QUIT -> {
                session.cancelActiveStatement();
                return false;
            }
            case COM_PING -> {
                writePacketAndFlush(ctx, new OkPacket(0, 0, session.serverStatusFlags(), 0, "", session.capabilities().flags()));
                return true;
            }
            case COM_INIT_DB -> {
                handleInitDb(ctx, session, MysqlPacketBuffer.readEofString(payload));
                return true;
            }
            case COM_QUERY -> {
                handleQuery(ctx, session, trimTrailingNul(MysqlPacketBuffer.readEofString(payload)));
                return true;
            }
            case COM_FIELD_LIST -> {
                handleFieldList(ctx, session, MysqlPacketBuffer.readEofString(payload));
                return true;
            }
            case COM_SET_OPTION -> {
                writePacketAndFlush(ctx, new OkPacket(0, 0, session.serverStatusFlags(), 0, "", session.capabilities().flags()));
                return true;
            }
            case COM_RESET_CONNECTION -> {
                handleResetConnection(ctx, session);
                return true;
            }
            case COM_STATISTICS -> {
                String statText = String.format(
                        "Uptime: %d  Threads: %d  Questions: %d",
                        stats.getUptimeSeconds(),
                        stats.getActiveConnections(),
                        stats.getTotalQueries());
                ByteBuf buf = ctx.alloc().buffer();
                MysqlPacketBuffer.writeEofString(buf, statText);
                ctx.writeAndFlush(buf);
                return true;
            }
            default -> {
                writePacketAndFlush(ctx, new ErrPacket(1047, "08S01", "Unsupported command: " + command.get()));
                return true;
            }
        }
    }

    private void handleInitDb(ChannelHandlerContext ctx, MysqlSession session, String dbName)
    {
        String trimmed = trimTrailingNul(dbName).trim();
        if (trimmed.isEmpty()) {
            writePacketAndFlush(ctx, new ErrPacket(1049, "42000", "Unknown database ''"));
            return;
        }

        List<String> parts = MysqlMetadataQueryHandler.splitQualifiedIdentifier(trimmed);
        if (parts.size() >= 2) {
            String useTarget = quoteIdentifier(parts.get(0)) + "." + quoteIdentifier(parts.get(1));
            statementExecutor.executeInitDb(ctx, session, "USE " + useTarget);
            return;
        }

        String singleName = parts.get(0);
        Optional<String> currentCatalog = session.clientSession().getCatalog()
                .or(() -> Optional.ofNullable(config.getDefaultCatalog()).filter(s -> !s.isEmpty()));

        if (currentCatalog.isPresent()) {
            String useSql = "USE " + quoteIdentifier(currentCatalog.get()) + "." + quoteIdentifier(singleName);
            Optional<TrinoErrorMapper.MysqlError> err = statementExecutor.tryExecuteInitDb(session, useSql);
            if (err.isEmpty()) {
                writePacketAndFlush(ctx, new OkPacket(0, 0, session.serverStatusFlags(), 0, "", session.capabilities().flags()));
                return;
            }
            // Fallback: check if singleName is actually a catalog name
            if (trySwitchToCatalog(session, singleName).isEmpty()) {
                writePacketAndFlush(ctx, new OkPacket(0, 0, session.serverStatusFlags(), 0, "", session.capabilities().flags()));
                return;
            }
            writePacketAndFlush(ctx, err.get().toErrPacket());
            return;
        }

        // No current catalog set: interpret singleName as a catalog name
        Optional<TrinoErrorMapper.MysqlError> catErr = trySwitchToCatalog(session, singleName);
        if (catErr.isEmpty()) {
            writePacketAndFlush(ctx, new OkPacket(0, 0, session.serverStatusFlags(), 0, "", session.capabilities().flags()));
        }
        else {
            writePacketAndFlush(ctx, catErr.get().toErrPacket());
        }
    }

    private Optional<TrinoErrorMapper.MysqlError> trySwitchToCatalog(MysqlSession session, String catalogName)
    {
        try {
            InternalQueryResult res = statementExecutor.executeInternalQuery(
                    session,
                    "SHOW SCHEMAS FROM " + quoteIdentifier(catalogName));
            List<String> schemas = new ArrayList<>(res.rows().size());
            for (List<Object> row : res.rows()) {
                if (!row.isEmpty() && row.get(0) != null) {
                    schemas.add(String.valueOf(row.get(0)));
                }
            }
            ClientSession.Builder builder = ClientSession.builder(session.clientSession())
                    .catalog(catalogName);
            if (!schemas.isEmpty()) {
                builder.schema(MysqlMetadataQueryHandler.pickDefaultSchema(schemas));
            }
            else if (config.getDefaultSchema() != null && !config.getDefaultSchema().isEmpty()) {
                builder.schema(config.getDefaultSchema());
            }
            session.setClientSession(builder.build());
            return Optional.empty();
        }
        catch (MysqlQueryException e) {
            return Optional.of(e.mysqlError());
        }
    }

    private void handleQuery(ChannelHandlerContext ctx, MysqlSession session, String rawSql)
    {
        Matcher killMatcher = KILL_PATTERN.matcher(rawSql.trim());
        if (killMatcher.matches()) {
            handleKill(ctx, session, killMatcher.group(1), Integer.parseInt(killMatcher.group(2)));
            return;
        }

        String sql = MysqlSqlPreprocessor.preprocess(rawSql);

        Optional<MysqlSystemVariableHandler.InterceptedResult> intercepted = systemVariableHandler.tryHandle(sql, session);
        if (intercepted.isPresent()) {
            writeInterceptedResult(ctx, session, intercepted.get());
            return;
        }

        try {
            Optional<MysqlSystemVariableHandler.InterceptedResult> metadataResult = metadataQueryHandler.tryHandle(sql, session);
            if (metadataResult.isPresent()) {
                writeInterceptedResult(ctx, session, metadataResult.get());
                return;
            }
        }
        catch (MysqlQueryException e) {
            writePacketAndFlush(ctx, e.mysqlError().toErrPacket());
            return;
        }

        // If client runs `USE <single_identifier>` when no session catalog is set, resolve catalog+schema
        Matcher useSingleMatcher = USE_SINGLE_IDENTIFIER_PATTERN.matcher(sql.trim());
        if (useSingleMatcher.matches() && session.clientSession().getCatalog().isEmpty()) {
            handleInitDb(ctx, session, useSingleMatcher.group(1));
            return;
        }

        statementExecutor.executeQuery(ctx, session, sql);
    }

    private void handleFieldList(ChannelHandlerContext ctx, MysqlSession session, String payloadStr)
    {
        int nulIdx = payloadStr.indexOf('\0');
        String tableName = (nulIdx >= 0 ? payloadStr.substring(0, nulIdx) : payloadStr).trim();
        if (tableName.isEmpty()) {
            writeFieldListEnd(ctx, session);
            return;
        }

        try {
            String qualified = MysqlMetadataQueryHandler.qualifyTableReference(tableName, Optional.empty());
            InternalQueryResult res = statementExecutor.executeInternalQuery(session, "DESCRIBE " + qualified);
            String schema = session.clientSession().getSchema().orElse("");
            for (List<Object> row : res.rows()) {
                String colName = !row.isEmpty() && row.get(0) != null ? String.valueOf(row.get(0)) : "";
                writePacket(ctx, TrinoToMysqlTypeMapper.stringColumn(schema, tableName, colName));
            }
            writeFieldListEnd(ctx, session);
        }
        catch (MysqlQueryException e) {
            writePacketAndFlush(ctx, e.mysqlError().toErrPacket());
        }
    }

    private static void writeFieldListEnd(ChannelHandlerContext ctx, MysqlSession session)
    {
        if (session.capabilities().isDeprecatedEof()) {
            writePacketAndFlush(ctx, new ResultSetEndPacket(session.serverStatusFlags(), 0));
        }
        else {
            writePacketAndFlush(ctx, new EofPacket(0, session.serverStatusFlags()));
        }
    }

    private void handleKill(ChannelHandlerContext ctx, MysqlSession session, String kind, int targetId)
    {
        Optional<MysqlSession> target = sessionRegistry.get(targetId);
        if (target.isEmpty()) {
            writePacketAndFlush(ctx, new ErrPacket(1094, "HY000", "Unknown thread id: " + targetId));
            return;
        }
        if ("QUERY".equalsIgnoreCase(kind)) {
            target.get().cancelActiveStatement();
        }
        else {
            target.get().closeConnection();
        }
        writePacketAndFlush(ctx, new OkPacket(0, 0, session.serverStatusFlags(), 0, "", session.capabilities().flags()));
    }

    private void handleResetConnection(ChannelHandlerContext ctx, MysqlSession session)
    {
        session.cancelActiveStatement();
        ClientSession current = session.clientSession();
        ClientSession reset = ClientSession.builder()
                .server(current.getServer())
                .user(current.getUser())
                .source(current.getSource())
                .catalog(current.getCatalog().orElse(null))
                .schema(current.getSchema().orElse(null))
                .timeZone(current.getTimeZone())
                .clientRequestTimeout(current.getClientRequestTimeout())
                .build();
        session.setClientSession(reset);
        writePacketAndFlush(ctx, new OkPacket(0, 0, session.serverStatusFlags(), 0, "", session.capabilities().flags()));
    }

    private static void writeInterceptedResult(
            ChannelHandlerContext ctx,
            MysqlSession session,
            MysqlSystemVariableHandler.InterceptedResult result)
    {
        if (!result.isResultSet()) {
            writePacketAndFlush(ctx, new OkPacket(0, 0, session.serverStatusFlags(), 0, "", session.capabilities().flags()));
            return;
        }

        ByteBuf countBuf = ctx.alloc().buffer(9);
        MysqlPacketBuffer.writeLenEncInt(countBuf, result.columnNames().size());
        ctx.write(countBuf);

        for (String colName : result.columnNames()) {
            writePacket(ctx, TrinoToMysqlTypeMapper.stringColumn(colName));
        }
        if (!session.capabilities().isDeprecatedEof()) {
            writePacket(ctx, new EofPacket(0, session.serverStatusFlags()));
        }

        for (List<String> row : result.rows()) {
            ByteBuf rowBuf = ctx.alloc().buffer();
            MysqlTextResultEncoder.encodeStringRow(rowBuf, row);
            ctx.write(rowBuf);
        }

        if (session.capabilities().isDeprecatedEof()) {
            writePacketAndFlush(ctx, new ResultSetEndPacket(session.serverStatusFlags(), 0));
        }
        else {
            writePacketAndFlush(ctx, new EofPacket(0, session.serverStatusFlags()));
        }
    }

    private static String trimTrailingNul(String s)
    {
        int end = s.length();
        while (end > 0 && s.charAt(end - 1) == '\0') {
            end--;
        }
        return s.substring(0, end);
    }
}
