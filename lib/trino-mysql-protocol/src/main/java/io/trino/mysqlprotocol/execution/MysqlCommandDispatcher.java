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
import io.trino.mysqlprotocol.metrics.MysqlProtocolStats;
import io.trino.mysqlprotocol.protocol.MysqlCommand;
import io.trino.mysqlprotocol.protocol.MysqlPacketBuffer;
import io.trino.mysqlprotocol.protocol.packet.EofPacket;
import io.trino.mysqlprotocol.protocol.packet.ErrPacket;
import io.trino.mysqlprotocol.protocol.packet.OkPacket;
import io.trino.mysqlprotocol.protocol.packet.ResultSetEndPacket;
import io.trino.mysqlprotocol.server.MysqlSessionRegistry;
import io.trino.mysqlprotocol.session.MysqlAuthenticator;
import io.trino.mysqlprotocol.session.MysqlSession;

import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    private final TrinoStatementExecutor statementExecutor;
    private final MysqlSystemVariableHandler systemVariableHandler;
    private final MysqlSessionRegistry sessionRegistry;
    private final MysqlProtocolConfig config;
    private final MysqlProtocolStats stats;

    @Inject
    public MysqlCommandDispatcher(
            TrinoStatementExecutor statementExecutor,
            MysqlSystemVariableHandler systemVariableHandler,
            MysqlSessionRegistry sessionRegistry,
            MysqlProtocolConfig config,
            MysqlProtocolStats stats)
    {
        this.statementExecutor = requireNonNull(statementExecutor, "statementExecutor is null");
        this.systemVariableHandler = requireNonNull(systemVariableHandler, "systemVariableHandler is null");
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
        MysqlAuthenticator.CatalogSchema target = MysqlAuthenticator.resolveCatalogSchema(Optional.of(trimmed), config);
        String useTarget;
        if (target.catalog().isPresent() && target.schema().isPresent()) {
            useTarget = quoteIdentifier(target.catalog().get()) + "." + quoteIdentifier(target.schema().get());
        }
        else if (target.schema().isPresent()) {
            useTarget = quoteIdentifier(target.schema().get());
        }
        else {
            writePacketAndFlush(ctx, new ErrPacket(1049, "42000", "Cannot resolve schema for database '" + trimmed + "'"));
            return;
        }
        statementExecutor.executeInitDb(ctx, session, "USE " + useTarget);
    }

    private void handleQuery(ChannelHandlerContext ctx, MysqlSession session, String sql)
    {
        Matcher killMatcher = KILL_PATTERN.matcher(sql.trim());
        if (killMatcher.matches()) {
            handleKill(ctx, session, killMatcher.group(1), Integer.parseInt(killMatcher.group(2)));
            return;
        }

        Optional<MysqlSystemVariableHandler.InterceptedResult> intercepted = systemVariableHandler.tryHandle(sql, session);
        if (intercepted.isPresent()) {
            writeInterceptedResult(ctx, session, intercepted.get());
            return;
        }

        statementExecutor.executeQuery(ctx, session, sql);
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

    private static String quoteIdentifier(String identifier)
    {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }
}
