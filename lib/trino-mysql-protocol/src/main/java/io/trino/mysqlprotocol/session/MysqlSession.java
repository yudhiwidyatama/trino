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
package io.trino.mysqlprotocol.session;

import io.netty.channel.Channel;
import io.trino.client.ClientSession;
import io.trino.client.StatementClient;
import io.trino.mysqlprotocol.protocol.MysqlCapability;
import io.trino.mysqlprotocol.protocol.MysqlServerStatusFlag;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

/**
 * Per-connection MySQL session holding negotiated wire capabilities, the mutable reference to
 * Trino's immutable {@link ClientSession}, and the currently executing {@link StatementClient}.
 */
public class MysqlSession
{
    private final int connectionId;
    private final MysqlCapability capabilities;
    private final Optional<String> basicAuthPassword;
    private final AtomicReference<ClientSession> clientSession;
    private final AtomicReference<StatementClient> activeStatement = new AtomicReference<>();
    private final AtomicReference<Channel> channel = new AtomicReference<>();

    public MysqlSession(
            int connectionId,
            MysqlCapability capabilities,
            ClientSession initialSession,
            Optional<String> basicAuthPassword)
    {
        this.connectionId = connectionId;
        this.capabilities = requireNonNull(capabilities, "capabilities is null");
        this.clientSession = new AtomicReference<>(requireNonNull(initialSession, "initialSession is null"));
        this.basicAuthPassword = requireNonNull(basicAuthPassword, "basicAuthPassword is null");
    }

    public int connectionId()
    {
        return connectionId;
    }

    public MysqlCapability capabilities()
    {
        return capabilities;
    }

    public Optional<String> basicAuthPassword()
    {
        return basicAuthPassword;
    }

    public ClientSession clientSession()
    {
        return clientSession.get();
    }

    public void setClientSession(ClientSession updated)
    {
        clientSession.set(requireNonNull(updated, "updated is null"));
    }

    public int serverStatusFlags()
    {
        ClientSession current = clientSession.get();
        int flags = MysqlServerStatusFlag.SERVER_STATUS_AUTOCOMMIT;
        if (current.getTransactionId() != null && !"NONE".equalsIgnoreCase(current.getTransactionId())) {
            flags |= MysqlServerStatusFlag.SERVER_STATUS_IN_TRANS;
        }
        return flags;
    }

    public void setActiveStatement(StatementClient statementClient)
    {
        activeStatement.set(statementClient);
    }

    public void clearActiveStatement()
    {
        activeStatement.set(null);
    }

    public void cancelActiveStatement()
    {
        StatementClient current = activeStatement.getAndSet(null);
        if (current != null) {
            try {
                current.close();
            }
            catch (RuntimeException ignored) {
                // best-effort cancel
            }
        }
    }

    public void attachChannel(Channel ch)
    {
        channel.set(ch);
    }

    public void closeConnection()
    {
        cancelActiveStatement();
        Channel ch = channel.get();
        if (ch != null && ch.isOpen()) {
            ch.close();
        }
    }
}
