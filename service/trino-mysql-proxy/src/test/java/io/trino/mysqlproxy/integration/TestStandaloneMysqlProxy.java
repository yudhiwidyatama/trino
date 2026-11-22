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
package io.trino.mysqlproxy.integration;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.jmx.testing.TestingJmxModule;
import io.airlift.log.Logging;
import io.airlift.node.testing.TestingNodeModule;
import io.trino.mysqlprotocol.server.MysqlNettyServer;
import io.trino.mysqlproxy.MysqlProxyStandaloneModule;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.testing.TestingTrinoServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * End-to-end integration test verifying Standalone Proxy Mode:
 * MysqlProxyStandaloneModule runs independently in front of an unmodified Trino Coordinator
 * while Trino's native HTTP REST port simultaneously serves direct Trino JDBC clients.
 */
@TestInstance(PER_CLASS)
public class TestStandaloneMysqlProxy
{
    private TestingTrinoServer trinoServer;
    private LifeCycleManager proxyLifeCycleManager;
    private int proxyMysqlPort;

    @BeforeAll
    void setUp()
    {
        Logging.initialize();
        trinoServer = TestingTrinoServer.create();
        trinoServer.installPlugin(new TpchPlugin());
        trinoServer.createCatalog("tpch", "tpch");
        trinoServer.installPlugin(new MemoryPlugin());
        trinoServer.createCatalog("memory", "memory");
        trinoServer.refreshNodes();

        Bootstrap app = new Bootstrap(
                new TestingNodeModule("test"),
                new TestingJmxModule(),
                new MysqlProxyStandaloneModule());

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperty("mysql-protocol.listen-host", "127.0.0.1")
                .setRequiredConfigurationProperty("mysql-protocol.listen-port", "0")
                .setRequiredConfigurationProperty("mysql-protocol.trino-uri", trinoServer.getBaseUrl().toString())
                .quiet()
                .initialize();

        proxyLifeCycleManager = injector.getInstance(LifeCycleManager.class);
        proxyMysqlPort = injector.getInstance(MysqlNettyServer.class).getBoundPort();
    }

    @AfterAll
    void tearDown()
            throws Exception
    {
        if (proxyLifeCycleManager != null) {
            proxyLifeCycleManager.stop();
        }
        if (trinoServer != null) {
            trinoServer.close();
        }
    }

    @Test
    void testStandaloneProxyAndDirectTrinoHttpInParallel()
            throws Exception
    {
        try (Connection mysqlConn = openMysqlConnection("tpch.tiny");
                Connection trinoConn = openTrinoJdbcConnection();
                Statement mysqlStmt = mysqlConn.createStatement();
                Statement trinoStmt = trinoConn.createStatement()) {
            try (ResultSet mysqlRs = mysqlStmt.executeQuery("SELECT count(*) FROM nation");
                    ResultSet trinoRs = trinoStmt.executeQuery("SELECT count(*) FROM nation")) {
                assertThat(mysqlRs.next()).isTrue();
                assertThat(trinoRs.next()).isTrue();
                assertThat(mysqlRs.getLong(1)).isEqualTo(25L);
                assertThat(trinoRs.getLong(1)).isEqualTo(25L);
            }

            // Switch schema via USE and verify cross-protocol visibility via memory catalog
            mysqlStmt.execute("USE memory.default");
            mysqlStmt.execute("CREATE TABLE proxy_items (id INTEGER, label VARCHAR)");
            mysqlStmt.executeUpdate("INSERT INTO proxy_items VALUES (10, 'from-mysql')");

            try (ResultSet trinoRs = trinoStmt.executeQuery("SELECT id, label FROM memory.default.proxy_items")) {
                assertThat(trinoRs.next()).isTrue();
                assertThat(trinoRs.getInt(1)).isEqualTo(10);
                assertThat(trinoRs.getString(2)).isEqualTo("from-mysql");
                assertThat(trinoRs.next()).isFalse();
            }
        }
    }

    private Connection openTrinoJdbcConnection()
            throws SQLException
    {
        String url = String.format("jdbc:trino://%s:%d/tpch/tiny",
                trinoServer.getAddress().getHost(),
                trinoServer.getAddress().getPort());
        Properties props = new Properties();
        props.setProperty("user", "direct_trino_user");
        return DriverManager.getConnection(url, props);
    }

    private Connection openMysqlConnection(String database)
            throws SQLException
    {
        String url = String.format("jdbc:mysql://127.0.0.1:%d/%s", proxyMysqlPort, database);
        Properties props = new Properties();
        props.setProperty("user", "proxy_mysql_user");
        props.setProperty("password", "");
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");
        return DriverManager.getConnection(url, props);
    }
}
