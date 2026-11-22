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
package io.trino.mysqlprotocol.server;

import io.airlift.log.Logger;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.IdleStateEvent;
import io.trino.mysqlprotocol.MysqlProtocolConfig;
import io.trino.mysqlprotocol.execution.MysqlCommandDispatcher;
import io.trino.mysqlprotocol.metrics.MysqlProtocolStats;
import io.trino.mysqlprotocol.protocol.MysqlCapability;
import io.trino.mysqlprotocol.protocol.MysqlServerStatusFlag;
import io.trino.mysqlprotocol.protocol.packet.AuthSwitchRequestPacket;
import io.trino.mysqlprotocol.protocol.packet.ErrPacket;
import io.trino.mysqlprotocol.protocol.packet.HandshakeResponse41Packet;
import io.trino.mysqlprotocol.protocol.packet.HandshakeV10Packet;
import io.trino.mysqlprotocol.protocol.packet.MysqlPacket;
import io.trino.mysqlprotocol.protocol.packet.OkPacket;
import io.trino.mysqlprotocol.protocol.packet.SslRequestPacket;
import io.trino.mysqlprotocol.session.MysqlAuthenticator;
import io.trino.mysqlprotocol.session.MysqlSession;

import java.security.SecureRandom;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

import static java.util.Objects.requireNonNull;

/**
 * Per-connection MySQL wire protocol handler.
 * Mirrors Apache Doris's AcceptListener + ReadListener lifecycle:
 * - Sends HandshakeV10 on connect
 * - Handles optional TLS upgrade and auth plugin switch
 * - Suspends auto-read while executing each command on the query thread pool so commands serialize per connection
 */
public class MysqlConnectionHandler
        extends SimpleChannelInboundHandler<ByteBuf>
{
    private static final Logger log = Logger.get(MysqlConnectionHandler.class);
    private static final SecureRandom RANDOM = new SecureRandom();

    private enum State
    {
        AWAITING_HANDSHAKE_RESPONSE,
        AWAITING_AUTH_SWITCH_RESPONSE,
        COMMAND,
        CLOSED
    }

    private final MysqlProtocolConfig config;
    private final MysqlAuthenticator authenticator;
    private final MysqlCommandDispatcher dispatcher;
    private final MysqlSessionRegistry sessionRegistry;
    private final MysqlProtocolStats stats;
    private final MysqlSequenceId sequenceId;
    private final ExecutorService queryExecutor;
    private final Optional<SslContext> sslContext;

    private State state = State.AWAITING_HANDSHAKE_RESPONSE;
    private int connectionId;
    private int serverCapabilities;
    private boolean tlsActive;
    private HandshakeResponse41Packet pendingAuthResponse;
    private MysqlSession session;

    public MysqlConnectionHandler(
            MysqlProtocolConfig config,
            MysqlAuthenticator authenticator,
            MysqlCommandDispatcher dispatcher,
            MysqlSessionRegistry sessionRegistry,
            MysqlProtocolStats stats,
            MysqlSequenceId sequenceId,
            ExecutorService queryExecutor,
            Optional<SslContext> sslContext)
    {
        this.config = requireNonNull(config, "config is null");
        this.authenticator = requireNonNull(authenticator, "authenticator is null");
        this.dispatcher = requireNonNull(dispatcher, "dispatcher is null");
        this.sessionRegistry = requireNonNull(sessionRegistry, "sessionRegistry is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.sequenceId = requireNonNull(sequenceId, "sequenceId is null");
        this.queryExecutor = requireNonNull(queryExecutor, "queryExecutor is null");
        this.sslContext = requireNonNull(sslContext, "sslContext is null");
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx)
    {
        stats.connectionOpened();
        connectionId = sessionRegistry.allocateConnectionId();
        serverCapabilities = MysqlCapability.DEFAULT_SERVER_CAPABILITIES
                | (sslContext.isPresent() ? MysqlCapability.CLIENT_SSL : 0);

        byte[] scramble = new byte[HandshakeV10Packet.SCRAMBLE_LENGTH];
        for (int i = 0; i < scramble.length; i++) {
            scramble[i] = (byte) (33 + RANDOM.nextInt(93)); // printable non-NUL ASCII
        }

        HandshakeV10Packet handshake = new HandshakeV10Packet(
                config.getServerVersion(),
                connectionId,
                scramble,
                serverCapabilities,
                MysqlServerStatusFlag.SERVER_STATUS_AUTOCOMMIT,
                authenticator.defaultAuthPluginName());
        writePacketAndFlush(ctx, handshake);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg)
    {
        switch (state) {
            case AWAITING_HANDSHAKE_RESPONSE -> handleHandshakeResponse(ctx, msg);
            case AWAITING_AUTH_SWITCH_RESPONSE -> handleAuthSwitchResponse(ctx, msg);
            case COMMAND -> handleCommand(ctx, msg);
            case CLOSED -> {
                // ignore
            }
        }
    }

    private void handleHandshakeResponse(ChannelHandlerContext ctx, ByteBuf msg)
    {
        if (msg.readableBytes() < 4) {
            sendErrorAndClose(ctx, new ErrPacket(1043, "08S01", "Bad handshake"));
            return;
        }
        int clientFlags = msg.getIntLE(msg.readerIndex());
        MysqlCapability clientCap = new MysqlCapability(clientFlags);

        // Handle TLS upgrade packet (32 bytes, CLIENT_SSL set, before actual HandshakeResponse41)
        if (!tlsActive && clientCap.isSsl() && msg.readableBytes() == 32) {
            if (sslContext.isEmpty()) {
                sendErrorAndClose(ctx, new ErrPacket(1105, "HY000", "Client requested TLS/SSL, but server TLS is disabled"));
                return;
            }
            SslRequestPacket.readFrom(msg);
            ctx.pipeline().addFirst("ssl", sslContext.get().newHandler(ctx.alloc()));
            tlsActive = true;
            return;
        }

        HandshakeResponse41Packet authPacket;
        try {
            authPacket = HandshakeResponse41Packet.readFrom(msg);
        }
        catch (RuntimeException e) {
            sendErrorAndClose(ctx, new ErrPacket(1043, "08S01", "Bad handshake: " + e.getMessage()));
            return;
        }

        if (authenticator.requiresClearPasswordSwitch(authPacket)) {
            if (!tlsActive) {
                sendErrorAndClose(ctx, new ErrPacket(1045, "28000", "TLS is required for BASIC_CLEAR_PASSWORD authentication"));
                return;
            }
            pendingAuthResponse = authPacket;
            state = State.AWAITING_AUTH_SWITCH_RESPONSE;
            writePacketAndFlush(ctx, new AuthSwitchRequestPacket(HandshakeV10Packet.MYSQL_CLEAR_PASSWORD, new byte[0]));
            return;
        }

        completeAuthentication(ctx, authPacket, Optional.empty());
    }

    private void handleAuthSwitchResponse(ChannelHandlerContext ctx, ByteBuf msg)
    {
        byte[] clearPassword = new byte[msg.readableBytes()];
        msg.readBytes(clearPassword);
        HandshakeResponse41Packet authPacket = pendingAuthResponse;
        pendingAuthResponse = null;
        completeAuthentication(ctx, authPacket, Optional.of(clearPassword));
    }

    private void completeAuthentication(
            ChannelHandlerContext ctx,
            HandshakeResponse41Packet authPacket,
            Optional<byte[]> clearPasswordBytes)
    {
        MysqlSession authenticated;
        try {
            authenticated = authenticator.authenticate(connectionId, serverCapabilities, authPacket, clearPasswordBytes, tlsActive);
        }
        catch (MysqlAuthenticator.AuthenticationException e) {
            sendErrorAndClose(ctx, new ErrPacket(e.errorCode(), e.sqlState(), e.getMessage()));
            return;
        }

        authenticated.attachChannel(ctx.channel());
        if (!sessionRegistry.tryRegister(authenticated, config.getMaxConnections())) {
            sendErrorAndClose(ctx, new ErrPacket(1040, "08004", "Too many connections"));
            return;
        }

        this.session = authenticated;
        this.state = State.COMMAND;
        writePacketAndFlush(ctx, new OkPacket(
                0,
                0,
                authenticated.serverStatusFlags(),
                0,
                "",
                authenticated.capabilities().flags()));
        // Reset sequenceId to 0 so the first client command packet starts at sequence 0
        sequenceId.reset();
    }

    private void handleCommand(ChannelHandlerContext ctx, ByteBuf msg)
    {
        // Retain payload for async execution on the query thread pool and suspend reads on this connection
        ByteBuf retained = msg.retain();
        ctx.channel().config().setAutoRead(false);

        try {
            queryExecutor.execute(() -> {
                try {
                    boolean keepOpen = dispatcher.dispatch(ctx, session, retained);
                    if (keepOpen && ctx.channel().isActive()) {
                        ctx.executor().execute(() -> {
                            sequenceId.reset();
                            ctx.channel().config().setAutoRead(true);
                        });
                    }
                    else {
                        state = State.CLOSED;
                        ctx.close();
                    }
                }
                catch (Throwable t) {
                    log.warn(t, "Unhandled error processing MySQL command on connection %d", connectionId);
                    if (ctx.channel().isActive()) {
                        sendErrorAndClose(ctx, new ErrPacket(1105, "HY000", "Internal error: " + t.getMessage()));
                    }
                }
                finally {
                    retained.release();
                }
            });
        }
        catch (RejectedExecutionException e) {
            retained.release();
            sendErrorAndClose(ctx, new ErrPacket(1040, "08004", "Query executor overloaded"));
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt)
            throws Exception
    {
        if (evt instanceof IdleStateEvent) {
            log.debug("Closing idle MySQL connection %d", connectionId);
            ctx.close();
            return;
        }
        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx)
    {
        state = State.CLOSED;
        if (session != null) {
            session.cancelActiveStatement();
            sessionRegistry.unregister(connectionId);
        }
        stats.connectionClosed();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        log.debug(cause, "MySQL connection %d error", connectionId);
        if (ctx.channel().isActive()) {
            sendErrorAndClose(ctx, new ErrPacket(1105, "HY000", cause.getMessage()));
        }
        else {
            ctx.close();
        }
    }

    private void sendErrorAndClose(ChannelHandlerContext ctx, ErrPacket packet)
    {
        state = State.CLOSED;
        ByteBuf buf = ctx.alloc().buffer();
        packet.writeTo(buf);
        ctx.writeAndFlush(buf).addListener(ChannelFutureListener.CLOSE);
    }

    private static void writePacketAndFlush(ChannelHandlerContext ctx, MysqlPacket packet)
    {
        ByteBuf buf = ctx.alloc().buffer();
        packet.writeTo(buf);
        ctx.writeAndFlush(buf);
    }
}
