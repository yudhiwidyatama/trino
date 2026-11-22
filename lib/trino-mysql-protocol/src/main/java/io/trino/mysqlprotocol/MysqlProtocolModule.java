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
package io.trino.mysqlprotocol;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.trino.mysqlprotocol.execution.MysqlCommandDispatcher;
import io.trino.mysqlprotocol.execution.MysqlSystemVariableHandler;
import io.trino.mysqlprotocol.execution.TrinoStatementExecutor;
import io.trino.mysqlprotocol.metrics.MysqlProtocolStats;
import io.trino.mysqlprotocol.server.MysqlNettyServer;
import io.trino.mysqlprotocol.server.MysqlSessionRegistry;
import io.trino.mysqlprotocol.session.MysqlAuthenticator;
import io.trino.mysqlprotocol.session.TrinoClientFactory;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

/**
 * Reusable Guice module that wires the MySQL wire protocol server and Trino REST bridge.
 * Installers must bind {@link MysqlTargetUriProvider}.
 */
public class MysqlProtocolModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(MysqlProtocolConfig.class);
        binder.bind(MysqlProtocolStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(MysqlProtocolStats.class).withGeneratedName();

        binder.bind(MysqlSessionRegistry.class).in(Scopes.SINGLETON);
        binder.bind(TrinoClientFactory.class).in(Scopes.SINGLETON);
        binder.bind(MysqlAuthenticator.class).in(Scopes.SINGLETON);
        binder.bind(TrinoStatementExecutor.class).in(Scopes.SINGLETON);
        binder.bind(MysqlSystemVariableHandler.class).in(Scopes.SINGLETON);
        binder.bind(MysqlCommandDispatcher.class).in(Scopes.SINGLETON);
        binder.bind(MysqlNettyServer.class).in(Scopes.SINGLETON);
    }
}
