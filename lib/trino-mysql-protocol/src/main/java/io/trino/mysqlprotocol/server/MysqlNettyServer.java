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

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.timeout.IdleStateHandler;
import io.trino.mysqlprotocol.MysqlProtocolConfig;
import io.trino.mysqlprotocol.execution.MysqlCommandDispatcher;
import io.trino.mysqlprotocol.metrics.MysqlProtocolStats;
import io.trino.mysqlprotocol.session.MysqlAuthenticator;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import javax.net.ssl.KeyManagerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * Netty TCP server accepting MySQL wire-protocol connections.
 * Runs cleanly either embedded inside a Trino Coordinator (parallel with Jetty HTTP/S)
 * or inside the standalone trino-mysql-proxy service.
 */
public class MysqlNettyServer
{
    private static final Logger log = Logger.get(MysqlNettyServer.class);

    private final MysqlProtocolConfig config;
    private final MysqlAuthenticator authenticator;
    private final MysqlCommandDispatcher dispatcher;
    private final MysqlSessionRegistry sessionRegistry;
    private final MysqlProtocolStats stats;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ExecutorService queryExecutor;
    private Channel serverChannel;
    private volatile int boundPort = -1;

    @Inject
    public MysqlNettyServer(
            MysqlProtocolConfig config,
            MysqlAuthenticator authenticator,
            MysqlCommandDispatcher dispatcher,
            MysqlSessionRegistry sessionRegistry,
            MysqlProtocolStats stats)
    {
        this.config = requireNonNull(config, "config is null");
        this.authenticator = requireNonNull(authenticator, "authenticator is null");
        this.dispatcher = requireNonNull(dispatcher, "dispatcher is null");
        this.sessionRegistry = requireNonNull(sessionRegistry, "sessionRegistry is null");
        this.stats = requireNonNull(stats, "stats is null");
    }

    @PostConstruct
    public synchronized void start()
            throws InterruptedException, IOException, GeneralSecurityException
    {
        if (serverChannel != null) {
            return;
        }

        Optional<SslContext> sslContext = buildSslContext(config);
        int maxAllowedPacket = toIntExact(config.getMaxAllowedPacket().toBytes());
        long idleTimeoutSeconds = Math.max(1L, (long) config.getIdleTimeout().getValue(TimeUnit.SECONDS));

        bossGroup = new NioEventLoopGroup(1, daemonThreadsNamed("mysql-boss-%s"));
        workerGroup = new NioEventLoopGroup(config.getIoThreads(), daemonThreadsNamed("mysql-io-%s"));
        queryExecutor = new ThreadPoolExecutor(
                Math.min(16, config.getQueryThreads()),
                config.getQueryThreads(),
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(config.getMaxConnections()),
                daemonThreadsNamed("mysql-query-%s"));

        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childHandler(new ChannelInitializer<SocketChannel>()
                {
                    @Override
                    protected void initChannel(SocketChannel ch)
                    {
                        MysqlSequenceId sequenceId = new MysqlSequenceId();
                        ch.pipeline()
                                .addLast("idle", new IdleStateHandler(idleTimeoutSeconds, 0, 0, TimeUnit.SECONDS))
                                .addLast("decoder", new MysqlPacketDecoder(sequenceId, maxAllowedPacket))
                                .addLast("encoder", new MysqlPacketEncoder(sequenceId))
                                .addLast("handler", new MysqlConnectionHandler(
                                        config,
                                        authenticator,
                                        dispatcher,
                                        sessionRegistry,
                                        stats,
                                        sequenceId,
                                        queryExecutor,
                                        sslContext));
                    }
                });

        serverChannel = bootstrap.bind(config.getListenHost(), config.getListenPort()).sync().channel();
        boundPort = ((InetSocketAddress) serverChannel.localAddress()).getPort();
        log.info("MySQL wire protocol listener started on %s:%d", config.getListenHost(), boundPort);
    }

    public int getBoundPort()
    {
        checkState(boundPort >= 0, "MysqlNettyServer has not been started");
        return boundPort;
    }

    @PreDestroy
    public synchronized void stop()
    {
        if (serverChannel != null) {
            try {
                serverChannel.close().syncUninterruptibly();
            }
            finally {
                serverChannel = null;
            }
        }
        sessionRegistry.closeAll();
        if (queryExecutor != null) {
            queryExecutor.shutdownNow();
            queryExecutor = null;
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
            workerGroup = null;
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
            bossGroup = null;
        }
        boundPort = -1;
        log.info("MySQL wire protocol listener stopped");
    }

    private static Optional<SslContext> buildSslContext(MysqlProtocolConfig config)
            throws IOException, GeneralSecurityException
    {
        if (!config.isTlsEnabled()) {
            return Optional.empty();
        }
        checkState(config.getTlsKeystorePath() != null, "mysql-protocol.tls.keystore-path must be set when TLS is enabled");
        char[] password = config.getTlsKeystorePassword() == null ? new char[0] : config.getTlsKeystorePassword().toCharArray();
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try (InputStream in = new FileInputStream(config.getTlsKeystorePath())) {
            keyStore.load(in, password);
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, password);
        return Optional.of(SslContextBuilder.forServer(kmf).build());
    }
}
