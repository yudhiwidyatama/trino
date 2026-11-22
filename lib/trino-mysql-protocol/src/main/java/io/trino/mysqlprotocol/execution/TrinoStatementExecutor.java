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
import io.airlift.log.Logger;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.trino.client.Column;
import io.trino.client.QueryData;
import io.trino.client.QueryStatusInfo;
import io.trino.client.StatementClient;
import io.trino.client.StatementClientFactory;
import io.trino.mysqlprotocol.metrics.MysqlProtocolStats;
import io.trino.mysqlprotocol.protocol.MysqlPacketBuffer;
import io.trino.mysqlprotocol.protocol.packet.ColumnDefinition41Packet;
import io.trino.mysqlprotocol.protocol.packet.EofPacket;
import io.trino.mysqlprotocol.protocol.packet.MysqlPacket;
import io.trino.mysqlprotocol.protocol.packet.OkPacket;
import io.trino.mysqlprotocol.protocol.packet.ResultSetEndPacket;
import io.trino.mysqlprotocol.session.MysqlSession;
import io.trino.mysqlprotocol.session.TrinoClientFactory;
import io.trino.mysqlprotocol.session.TrinoSessionSynchronizer;
import okhttp3.OkHttpClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Executes SQL statements against Trino's /v1/statement REST API via {@link StatementClient}
 * and streams results incrementally into MySQL wire-protocol packets.
 */
public class TrinoStatementExecutor
{
    private static final Logger log = Logger.get(TrinoStatementExecutor.class);

    private final TrinoClientFactory clientFactory;
    private final MysqlProtocolStats stats;

    @Inject
    public TrinoStatementExecutor(TrinoClientFactory clientFactory, MysqlProtocolStats stats)
    {
        this.clientFactory = requireNonNull(clientFactory, "clientFactory is null");
        this.stats = requireNonNull(stats, "stats is null");
    }

    public void executeQuery(ChannelHandlerContext ctx, MysqlSession session, String sql)
    {
        stats.queryStarted();
        OkHttpClient httpClient = clientFactory.clientFor(session);
        boolean headerSent = false;
        List<Column> columns = null;
        long streamedRows = 0;

        try (StatementClient client = StatementClientFactory.newStatementClient(httpClient, session.clientSession(), sql)) {
            session.setActiveStatement(client);
            String schema = session.clientSession().getSchema().orElse("");

            while (client.isRunning()) {
                if (!ctx.channel().isActive()) {
                    client.close();
                    return;
                }

                QueryStatusInfo statusInfo = client.currentStatusInfo();
                boolean isUpdate = statusInfo.getUpdateType() != null;
                if (!headerSent && !isUpdate && statusInfo.getColumns() != null && !statusInfo.getColumns().isEmpty()) {
                    columns = statusInfo.getColumns();
                    writeColumnDefinitions(ctx, session, schema, columns);
                    headerSent = true;
                }

                if (headerSent && columns != null) {
                    QueryData data = client.currentData();
                    if (data != null && data.getData() != null) {
                        long pageRows = 0;
                        for (List<Object> row : data.getData()) {
                            ByteBuf rowBuf = ctx.alloc().buffer();
                            MysqlTextResultEncoder.encodeRow(rowBuf, columns, row);
                            ctx.write(rowBuf);
                            pageRows++;
                        }
                        if (pageRows > 0) {
                            streamedRows += pageRows;
                            stats.rowsStreamed(pageRows);
                            ctx.flush();
                        }
                    }
                }

                client.advance();
            }

            TrinoSessionSynchronizer.synchronize(session, client);

            QueryStatusInfo finalStatus = client.finalStatusInfo();
            if (client.isClientAborted()) {
                stats.queryFailed();
                writePacketAndFlush(ctx, new TrinoErrorMapper.MysqlError(1317, "70100", "Query execution was interrupted").toErrPacket());
                return;
            }
            if (client.isClientError() || finalStatus.getError() != null) {
                stats.queryFailed();
                writePacketAndFlush(ctx, TrinoErrorMapper.fromQueryError(finalStatus.getError()).toErrPacket());
                return;
            }

            // Capture columns if Trino only returned them on the final empty response of a SELECT
            if (!headerSent
                    && finalStatus.getUpdateType() == null
                    && finalStatus.getColumns() != null
                    && !finalStatus.getColumns().isEmpty()) {
                columns = finalStatus.getColumns();
                writeColumnDefinitions(ctx, session, schema, columns);
                headerSent = true;
            }

            int warnings = finalStatus.getWarnings() == null ? 0 : finalStatus.getWarnings().size();
            if (headerSent) {
                if (session.capabilities().isDeprecatedEof()) {
                    writePacketAndFlush(ctx, new ResultSetEndPacket(session.serverStatusFlags(), warnings));
                }
                else {
                    writePacketAndFlush(ctx, new EofPacket(warnings, session.serverStatusFlags()));
                }
            }
            else {
                long affectedRows = finalStatus.getUpdateCount() == null ? 0L : finalStatus.getUpdateCount();
                writePacketAndFlush(ctx, new OkPacket(
                        affectedRows,
                        0,
                        session.serverStatusFlags(),
                        warnings,
                        "",
                        session.capabilities().flags()));
            }
        }
        catch (Throwable t) {
            stats.queryFailed();
            log.debug(t, "Error executing Trino query: %s", sql);
            if (ctx.channel().isActive()) {
                writePacketAndFlush(ctx, TrinoErrorMapper.fromThrowable(t).toErrPacket());
            }
        }
        finally {
            session.clearActiveStatement();
        }
    }

    public void executeInitDb(ChannelHandlerContext ctx, MysqlSession session, String useSql)
    {
        Optional<TrinoErrorMapper.MysqlError> error = tryExecuteInitDb(session, useSql);
        if (error.isPresent()) {
            writePacketAndFlush(ctx, error.get().toErrPacket());
            return;
        }
        writePacketAndFlush(ctx, new OkPacket(0, 0, session.serverStatusFlags(), 0, "", session.capabilities().flags()));
    }

    public Optional<TrinoErrorMapper.MysqlError> tryExecuteInitDb(MysqlSession session, String useSql)
    {
        OkHttpClient httpClient = clientFactory.clientFor(session);
        try (StatementClient client = StatementClientFactory.newStatementClient(httpClient, session.clientSession(), useSql)) {
            session.setActiveStatement(client);
            while (client.isRunning()) {
                client.advance();
            }
            TrinoSessionSynchronizer.synchronize(session, client);
            QueryStatusInfo finalStatus = client.finalStatusInfo();
            if (client.isClientError() || finalStatus.getError() != null) {
                return Optional.of(TrinoErrorMapper.fromQueryError(finalStatus.getError()));
            }
            return Optional.empty();
        }
        catch (Throwable t) {
            return Optional.of(TrinoErrorMapper.fromThrowable(t));
        }
        finally {
            session.clearActiveStatement();
        }
    }

    public InternalQueryResult executeInternalQuery(MysqlSession session, String sql)
    {
        stats.queryStarted();
        OkHttpClient httpClient = clientFactory.clientFor(session);
        List<Column> columns = List.of();
        List<List<Object>> rows = new ArrayList<>();

        try (StatementClient client = StatementClientFactory.newStatementClient(httpClient, session.clientSession(), sql)) {
            session.setActiveStatement(client);
            while (client.isRunning()) {
                QueryStatusInfo statusInfo = client.currentStatusInfo();
                if (columns.isEmpty() && statusInfo.getColumns() != null && !statusInfo.getColumns().isEmpty()) {
                    columns = statusInfo.getColumns();
                }
                QueryData data = client.currentData();
                if (data != null && data.getData() != null) {
                    for (List<Object> row : data.getData()) {
                        rows.add(row);
                    }
                }
                client.advance();
            }

            TrinoSessionSynchronizer.synchronize(session, client);

            QueryStatusInfo finalStatus = client.finalStatusInfo();
            if (client.isClientAborted()) {
                stats.queryFailed();
                throw new MysqlQueryException(new TrinoErrorMapper.MysqlError(1317, "70100", "Query execution was interrupted"));
            }
            if (client.isClientError() || finalStatus.getError() != null) {
                stats.queryFailed();
                throw new MysqlQueryException(TrinoErrorMapper.fromQueryError(finalStatus.getError()));
            }
            if (columns.isEmpty() && finalStatus.getColumns() != null) {
                columns = finalStatus.getColumns();
            }
            return new InternalQueryResult(columns, rows);
        }
        catch (MysqlQueryException e) {
            throw e;
        }
        catch (Throwable t) {
            stats.queryFailed();
            throw new MysqlQueryException(TrinoErrorMapper.fromThrowable(t));
        }
        finally {
            session.clearActiveStatement();
        }
    }

    public record InternalQueryResult(List<Column> columns, List<List<Object>> rows) {}

    public static class MysqlQueryException
            extends RuntimeException
    {
        private final TrinoErrorMapper.MysqlError mysqlError;

        public MysqlQueryException(TrinoErrorMapper.MysqlError mysqlError)
        {
            super(mysqlError.message());
            this.mysqlError = requireNonNull(mysqlError, "mysqlError is null");
        }

        public TrinoErrorMapper.MysqlError mysqlError()
        {
            return mysqlError;
        }
    }

    private static void writeColumnDefinitions(ChannelHandlerContext ctx, MysqlSession session, String schema, List<Column> columns)
    {
        ByteBuf countBuf = ctx.alloc().buffer(9);
        MysqlPacketBuffer.writeLenEncInt(countBuf, columns.size());
        ctx.write(countBuf);

        for (Column column : columns) {
            ColumnDefinition41Packet colDef = TrinoToMysqlTypeMapper.toColumnDefinition(schema, "", column);
            writePacket(ctx, colDef);
        }

        if (!session.capabilities().isDeprecatedEof()) {
            writePacket(ctx, new EofPacket(0, session.serverStatusFlags()));
        }
        ctx.flush();
    }

    public static void writePacket(ChannelHandlerContext ctx, MysqlPacket packet)
    {
        ByteBuf buf = ctx.alloc().buffer();
        packet.writeTo(buf);
        ctx.write(buf);
    }

    public static void writePacketAndFlush(ChannelHandlerContext ctx, MysqlPacket packet)
    {
        writePacket(ctx, packet);
        ctx.flush();
    }
}
