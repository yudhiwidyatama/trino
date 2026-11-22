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

import com.google.inject.Key;
import io.airlift.log.Logging;
import io.trino.mysqlprotocol.server.MysqlNettyServer;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * End-to-end integration test verifying Embedded Coordinator Mode:
 * a single Trino Coordinator JVM runs BOTH its native Jetty HTTP REST server (Trino JDBC)
 * and the embedded Netty MySQL wire protocol listener (MySQL Connector/J) in parallel.
 */
@TestInstance(PER_CLASS)
public class TestEmbeddedMysqlProtocol
{
    private TestingTrinoServer server;
    private int mysqlPort;

    @BeforeAll
    void setUp()
    {
        Logging.initialize();
        server = TestingTrinoServer.builder()
                .setProperties(Map.of(
                        "mysql-protocol.enabled", "true",
                        "mysql-protocol.listen-host", "127.0.0.1",
                        "mysql-protocol.listen-port", "0"))
                .build();
        server.installPlugin(new TpchPlugin());
        server.createCatalog("tpch", "tpch");
        server.installPlugin(new MemoryPlugin());
        server.createCatalog("memory", "memory");
        server.refreshNodes();

        mysqlPort = server.getInstance(Key.get(MysqlNettyServer.class)).getBoundPort();
    }

    @AfterAll
    void tearDown()
            throws Exception
    {
        if (server != null) {
            server.close();
            server = null;
        }
    }

    @Test
    void testParallelTrinoHttpAndEmbeddedMysqlProtocol()
            throws Exception
    {
        String sql = "SELECT nationkey, name, regionkey FROM tpch.tiny.nation ORDER BY nationkey LIMIT 5";
        List<String> trinoJdbcRows = new ArrayList<>();
        List<String> mysqlJdbcRows = new ArrayList<>();

        try (Connection trinoConn = openTrinoJdbcConnection();
                Connection mysqlConn = openMysqlConnection("tpch.tiny")) {
            assertThat(mysqlConn.isValid(5)).isTrue();

            try (Statement trinoStmt = trinoConn.createStatement();
                    ResultSet trinoRs = trinoStmt.executeQuery(sql);
                    Statement mysqlStmt = mysqlConn.createStatement();
                    ResultSet mysqlRs = mysqlStmt.executeQuery(sql)) {
                while (trinoRs.next()) {
                    trinoJdbcRows.add(trinoRs.getLong(1) + "|" + trinoRs.getString(2) + "|" + trinoRs.getLong(3));
                }
                while (mysqlRs.next()) {
                    mysqlJdbcRows.add(mysqlRs.getLong(1) + "|" + mysqlRs.getString(2) + "|" + mysqlRs.getLong(3));
                }
            }
        }

        assertThat(trinoJdbcRows).hasSize(5);
        assertThat(mysqlJdbcRows).containsExactlyElementsOf(trinoJdbcRows);
    }

    @Test
    void testDdlDmlUseAndErrorMappingViaEmbeddedMysql()
            throws Exception
    {
        try (Connection mysqlConn = openMysqlConnection("memory.default");
                Statement stmt = mysqlConn.createStatement()) {
            stmt.execute("CREATE TABLE embedded_users (id BIGINT, username VARCHAR, active BOOLEAN)");
            int inserted = stmt.executeUpdate("INSERT INTO embedded_users VALUES (1, 'alice', true), (2, 'bob', false)");
            assertThat(inserted).isEqualTo(2);

            try (ResultSet rs = stmt.executeQuery("SELECT id, username, active FROM embedded_users ORDER BY id")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong(1)).isEqualTo(1L);
                assertThat(rs.getString(2)).isEqualTo("alice");
                assertThat(rs.getBoolean(3)).isTrue();

                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong(1)).isEqualTo(2L);
                assertThat(rs.getString(2)).isEqualTo("bob");
                assertThat(rs.getBoolean(3)).isFalse();
                assertThat(rs.next()).isFalse();
            }

            stmt.execute("DROP TABLE embedded_users");

            assertThatThrownBy(() -> stmt.executeQuery("SELECT * FROM embedded_users"))
                    .isInstanceOf(SQLException.class)
                    .satisfies(ex -> assertThat(((SQLException) ex).getErrorCode()).isEqualTo(1146));
        }
    }

    private Connection openTrinoJdbcConnection()
            throws SQLException
    {
        String url = String.format("jdbc:trino://%s:%d/tpch/tiny",
                server.getAddress().getHost(),
                server.getAddress().getPort());
        Properties props = new Properties();
        props.setProperty("user", "trino_user");
        return DriverManager.getConnection(url, props);
    }

    private Connection openMysqlConnection(String database)
            throws SQLException
    {
        String url = String.format("jdbc:mysql://127.0.0.1:%d/%s", mysqlPort, database);
        Properties props = new Properties();
        props.setProperty("user", "mysql_user");
        props.setProperty("password", "");
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");
        return DriverManager.getConnection(url, props);
    }
}
