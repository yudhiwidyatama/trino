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
package io.trino.mysqlprotocol.mock;

import io.trino.mysqlprotocol.MysqlProtocolConfig;
import io.trino.mysqlprotocol.execution.MysqlCommandDispatcher;
import io.trino.mysqlprotocol.execution.MysqlSystemVariableHandler;
import io.trino.mysqlprotocol.execution.TrinoStatementExecutor;
import io.trino.mysqlprotocol.metrics.MysqlProtocolStats;
import io.trino.mysqlprotocol.server.MysqlNettyServer;
import io.trino.mysqlprotocol.server.MysqlSessionRegistry;
import io.trino.mysqlprotocol.session.MysqlAuthenticator;
import io.trino.mysqlprotocol.session.TrinoClientFactory;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Outer-ring ATDD test using REAL MySQL Connector/J 8.3.0 against MysqlNettyServer
 * backed by OkHttp MockWebServer simulating Trino's /v1/statement REST API.
 */
class TestMysqlProtocolWithMockTrino
{
    private MockWebServer mockTrino;
    private MysqlNettyServer mysqlServer;

    @BeforeEach
    void setUp()
            throws Exception
    {
        mockTrino = new MockWebServer();
        mockTrino.start();

        MysqlProtocolConfig config = new MysqlProtocolConfig()
                .setEnabled(true)
                .setListenHost("127.0.0.1")
                .setListenPort(0);
        MysqlSessionRegistry registry = new MysqlSessionRegistry();
        MysqlProtocolStats stats = new MysqlProtocolStats();
        TrinoClientFactory clientFactory = new TrinoClientFactory(config);
        MysqlAuthenticator authenticator = new MysqlAuthenticator(config, () -> mockTrino.url("/").uri());
        TrinoStatementExecutor executor = new TrinoStatementExecutor(clientFactory, stats);
        MysqlSystemVariableHandler sysVarHandler = new MysqlSystemVariableHandler(config);
        MysqlCommandDispatcher dispatcher = new MysqlCommandDispatcher(executor, sysVarHandler, registry, config, stats);

        mysqlServer = new MysqlNettyServer(config, authenticator, dispatcher, registry, stats);
        mysqlServer.start();
    }

    @AfterEach
    void tearDown()
            throws Exception
    {
        if (mysqlServer != null) {
            mysqlServer.stop();
        }
        if (mockTrino != null) {
            mockTrino.shutdown();
        }
    }

    @Test
    void testConnectorJHandshakePingAndMultiPageSelect()
            throws Exception
    {
        // Page 1: queued -> returns nextUri with columns and first row
        String nextUrl = mockTrino.url("/v1/statement/executing/q1/slug/1").toString();
        String page1 = """
                {
                  "id": "q1",
                  "infoUri": "http://localhost/ui/query.html?q1",
                  "nextUri": "%s",
                  "columns": [
                    {"name": "id", "type": "bigint", "typeSignature": {"rawType": "bigint", "arguments": []}},
                    {"name": "name", "type": "varchar", "typeSignature": {"rawType": "varchar", "arguments": []}},
                    {"name": "active", "type": "boolean", "typeSignature": {"rawType": "boolean", "arguments": []}},
                    {"name": "amount", "type": "decimal(10,2)", "typeSignature": {"rawType": "decimal", "arguments": [{"kind": "LONG", "value": 10}, {"kind": "LONG", "value": 2}]}}
                  ],
                  "data": [[1, "alice", true, "12.34"]],
                  "stats": {"state": "RUNNING", "queued": false, "scheduled": true, "nodes": 1, "totalSplits": 1, "queuedSplits": 0, "runningSplits": 1, "completedSplits": 0, "cpuTimeMillis": 1, "wallTimeMillis": 1, "queuedTimeMillis": 0, "elapsedTimeMillis": 1, "processedRows": 1, "processedBytes": 10, "physicalInputBytes": 0, "peakMemoryBytes": 0, "spilledBytes": 0}
                }
                """.formatted(nextUrl);

        // Page 2: final page (no nextUri) with second row including NULL
        String page2 = """
                {
                  "id": "q1",
                  "infoUri": "http://localhost/ui/query.html?q1",
                  "columns": [
                    {"name": "id", "type": "bigint", "typeSignature": {"rawType": "bigint", "arguments": []}},
                    {"name": "name", "type": "varchar", "typeSignature": {"rawType": "varchar", "arguments": []}},
                    {"name": "active", "type": "boolean", "typeSignature": {"rawType": "boolean", "arguments": []}},
                    {"name": "amount", "type": "decimal(10,2)", "typeSignature": {"rawType": "decimal", "arguments": [{"kind": "LONG", "value": 10}, {"kind": "LONG", "value": 2}]}}
                  ],
                  "data": [[2, null, false, "99.00"]],
                  "stats": {"state": "FINISHED", "queued": false, "scheduled": true, "nodes": 1, "totalSplits": 1, "queuedSplits": 0, "runningSplits": 0, "completedSplits": 1, "cpuTimeMillis": 2, "wallTimeMillis": 2, "queuedTimeMillis": 0, "elapsedTimeMillis": 2, "processedRows": 2, "processedBytes": 20, "physicalInputBytes": 0, "peakMemoryBytes": 0, "spilledBytes": 0}
                }
                """;

        mockTrino.enqueue(new MockResponse().setHeader("Content-Type", "application/json").setBody(page1));
        mockTrino.enqueue(new MockResponse().setHeader("Content-Type", "application/json").setBody(page2));

        try (Connection conn = openConnection("tpch.tiny")) {
            assertThat(conn.isValid(5)).isTrue();

            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT id, name, active, amount FROM test_tbl")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong("id")).isEqualTo(1L);
                assertThat(rs.getString("name")).isEqualTo("alice");
                assertThat(rs.getBoolean("active")).isTrue();
                assertThat(rs.getBigDecimal("amount")).isEqualByComparingTo(new BigDecimal("12.34"));

                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong("id")).isEqualTo(2L);
                assertThat(rs.getString("name")).isNull();
                assertThat(rs.getBoolean("active")).isFalse();
                assertThat(rs.getBigDecimal("amount")).isEqualByComparingTo(new BigDecimal("99.00"));

                assertThat(rs.next()).isFalse();
            }
        }

        RecordedRequest firstReq = mockTrino.takeRequest();
        assertThat(firstReq.getMethod()).isEqualTo("POST");
        assertThat(firstReq.getPath()).isEqualTo("/v1/statement");
        assertThat(firstReq.getHeader("X-Trino-User")).isEqualTo("trino_tester");
        assertThat(firstReq.getHeader("X-Trino-Catalog")).isEqualTo("tpch");
        assertThat(firstReq.getHeader("X-Trino-Schema")).isEqualTo("tiny");
        assertThat(readBody(firstReq)).isEqualTo("SELECT id, name, active, amount FROM test_tbl");

        RecordedRequest secondReq = mockTrino.takeRequest();
        assertThat(secondReq.getMethod()).isEqualTo("GET");
        assertThat(secondReq.getPath()).isEqualTo("/v1/statement/executing/q1/slug/1");
    }

    @Test
    void testSessionHeaderSyncAndDmlUpdateCountAndErrorMapping()
            throws Exception
    {
        String useResponse = """
                {
                  "id": "q_use",
                  "infoUri": "http://localhost/ui/query.html?q_use",
                  "stats": {"state": "FINISHED", "queued": false, "scheduled": false, "nodes": 1, "totalSplits": 0, "queuedSplits": 0, "runningSplits": 0, "completedSplits": 0, "cpuTimeMillis": 0, "wallTimeMillis": 0, "queuedTimeMillis": 0, "elapsedTimeMillis": 1, "processedRows": 0, "processedBytes": 0, "physicalInputBytes": 0, "peakMemoryBytes": 0, "spilledBytes": 0}
                }
                """;
        String insertResponse = """
                {
                  "id": "q_ins",
                  "infoUri": "http://localhost/ui/query.html?q_ins",
                  "columns": [{"name": "rows", "type": "bigint", "typeSignature": {"rawType": "bigint", "arguments": []}}],
                  "data": [[7]],
                  "updateType": "INSERT",
                  "updateCount": 7,
                  "stats": {"state": "FINISHED", "queued": false, "scheduled": true, "nodes": 1, "totalSplits": 1, "queuedSplits": 0, "runningSplits": 0, "completedSplits": 1, "cpuTimeMillis": 1, "wallTimeMillis": 1, "queuedTimeMillis": 0, "elapsedTimeMillis": 1, "processedRows": 7, "processedBytes": 70, "physicalInputBytes": 0, "peakMemoryBytes": 0, "spilledBytes": 0}
                }
                """;
        String errResponse = """
                {
                  "id": "q_err",
                  "infoUri": "http://localhost/ui/query.html?q_err",
                  "error": {"message": "Table memory.default.missing does not exist", "errorCode": 46, "errorName": "TABLE_NOT_FOUND", "errorType": "USER_ERROR"},
                  "stats": {"state": "FAILED", "queued": false, "scheduled": false, "nodes": 1, "totalSplits": 0, "queuedSplits": 0, "runningSplits": 0, "completedSplits": 0, "cpuTimeMillis": 0, "wallTimeMillis": 0, "queuedTimeMillis": 0, "elapsedTimeMillis": 1, "processedRows": 0, "processedBytes": 0, "physicalInputBytes": 0, "peakMemoryBytes": 0, "spilledBytes": 0}
                }
                """;

        mockTrino.enqueue(new MockResponse()
                .setHeader("Content-Type", "application/json")
                .setHeader("X-Trino-Set-Catalog", "memory")
                .setHeader("X-Trino-Set-Schema", "default")
                .setBody(useResponse));
        mockTrino.enqueue(new MockResponse()
                .setHeader("Content-Type", "application/json")
                .setBody(insertResponse));
        mockTrino.enqueue(new MockResponse()
                .setHeader("Content-Type", "application/json")
                .setBody(errResponse));

        try (Connection conn = openConnection("")) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("USE memory.default");
                int updated = stmt.executeUpdate("INSERT INTO t VALUES (1)");
                assertThat(updated).isEqualTo(7);

                assertThatThrownBy(() -> stmt.executeQuery("SELECT * FROM missing"))
                        .isInstanceOf(SQLException.class)
                        .satisfies(ex -> {
                            SQLException sqlEx = (SQLException) ex;
                            assertThat(sqlEx.getErrorCode()).isEqualTo(1146);
                            assertThat(sqlEx.getSQLState()).isEqualTo("42S02");
                            assertThat(sqlEx.getMessage()).contains("does not exist");
                        });
            }
        }

        // Verify the second request carried X-Trino-Catalog and X-Trino-Schema updated from the first response
        mockTrino.takeRequest(); // USE
        RecordedRequest insertReq = mockTrino.takeRequest();
        assertThat(insertReq.getHeader("X-Trino-Catalog")).isEqualTo("memory");
        assertThat(insertReq.getHeader("X-Trino-Schema")).isEqualTo("default");
    }

    @Test
    void testSequelProBackquotedQueriesAndMetadataCompatibility()
            throws Exception
    {
        String showTablesResponse = """
                {
                  "id": "q_tables",
                  "infoUri": "http://localhost/ui/query.html?q_tables",
                  "columns": [{"name": "Table", "type": "varchar", "typeSignature": {"rawType": "varchar", "arguments": []}}],
                  "data": [["nation"], ["region"]],
                  "stats": {"state": "FINISHED", "queued": false, "scheduled": true, "nodes": 1, "totalSplits": 1, "queuedSplits": 0, "runningSplits": 0, "completedSplits": 1, "cpuTimeMillis": 1, "wallTimeMillis": 1, "queuedTimeMillis": 0, "elapsedTimeMillis": 1, "processedRows": 2, "processedBytes": 20, "physicalInputBytes": 0, "peakMemoryBytes": 0, "spilledBytes": 0}
                }
                """;
        String describeResponse = """
                {
                  "id": "q_desc",
                  "infoUri": "http://localhost/ui/query.html?q_desc",
                  "columns": [
                    {"name": "Column", "type": "varchar", "typeSignature": {"rawType": "varchar", "arguments": []}},
                    {"name": "Type", "type": "varchar", "typeSignature": {"rawType": "varchar", "arguments": []}},
                    {"name": "Extra", "type": "varchar", "typeSignature": {"rawType": "varchar", "arguments": []}},
                    {"name": "Comment", "type": "varchar", "typeSignature": {"rawType": "varchar", "arguments": []}}
                  ],
                  "data": [
                    ["nationkey", "bigint", "", ""],
                    ["name", "varchar(25)", "", ""]
                  ],
                  "stats": {"state": "FINISHED", "queued": false, "scheduled": true, "nodes": 1, "totalSplits": 1, "queuedSplits": 0, "runningSplits": 0, "completedSplits": 1, "cpuTimeMillis": 1, "wallTimeMillis": 1, "queuedTimeMillis": 0, "elapsedTimeMillis": 1, "processedRows": 2, "processedBytes": 20, "physicalInputBytes": 0, "peakMemoryBytes": 0, "spilledBytes": 0}
                }
                """;
        String selectResponse = """
                {
                  "id": "q_sel",
                  "infoUri": "http://localhost/ui/query.html?q_sel",
                  "columns": [
                    {"name": "nationkey", "type": "bigint", "typeSignature": {"rawType": "bigint", "arguments": []}},
                    {"name": "name", "type": "varchar(25)", "typeSignature": {"rawType": "varchar", "arguments": [{"kind": "LONG", "value": 25}]}}
                  ],
                  "data": [[0, "ALGERIA"]],
                  "stats": {"state": "FINISHED", "queued": false, "scheduled": true, "nodes": 1, "totalSplits": 1, "queuedSplits": 0, "runningSplits": 0, "completedSplits": 1, "cpuTimeMillis": 1, "wallTimeMillis": 1, "queuedTimeMillis": 0, "elapsedTimeMillis": 1, "processedRows": 1, "processedBytes": 10, "physicalInputBytes": 0, "peakMemoryBytes": 0, "spilledBytes": 0}
                }
                """;

        mockTrino.enqueue(new MockResponse().setHeader("Content-Type", "application/json").setBody(showTablesResponse));
        mockTrino.enqueue(new MockResponse().setHeader("Content-Type", "application/json").setBody(describeResponse));
        mockTrino.enqueue(new MockResponse().setHeader("Content-Type", "application/json").setBody(describeResponse));
        mockTrino.enqueue(new MockResponse().setHeader("Content-Type", "application/json").setBody(selectResponse));

        try (Connection conn = openConnection("tpch.tiny");
                Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SHOW FULL TABLES FROM `tiny`")) {
                assertThat(rs.getMetaData().getColumnCount()).isEqualTo(2);
                assertThat(rs.getMetaData().getColumnLabel(1)).isEqualTo("Tables_in_tiny");
                assertThat(rs.getMetaData().getColumnLabel(2)).isEqualTo("Table_type");
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString(1)).isEqualTo("nation");
                assertThat(rs.getString(2)).isEqualTo("BASE TABLE");
            }

            try (ResultSet rs = stmt.executeQuery("SHOW CREATE TABLE `nation`")) {
                assertThat(rs.getMetaData().getColumnCount()).isEqualTo(2);
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("Table")).isEqualTo("nation");
                assertThat(rs.getString("Create Table"))
                        .contains("CREATE TABLE `nation`")
                        .contains("`nationkey` bigint(20) DEFAULT NULL")
                        .contains("`name` varchar(25) DEFAULT NULL");
            }

            try (ResultSet rs = stmt.executeQuery("SHOW FULL COLUMNS FROM `nation`")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("Field")).isEqualTo("nationkey");
                assertThat(rs.getString("Type")).isEqualTo("bigint(20)");
            }

            try (ResultSet rs = stmt.executeQuery("SELECT `nationkey`, `name` FROM `nation` LIMIT 0, 1000;")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong(1)).isEqualTo(0L);
                assertThat(rs.getString(2)).isEqualTo("ALGERIA");
            }
        }

        assertThat(readBody(mockTrino.takeRequest())).isEqualTo("SHOW TABLES FROM \"tpch\".\"tiny\"");
        assertThat(readBody(mockTrino.takeRequest())).isEqualTo("DESCRIBE \"nation\"");
        assertThat(readBody(mockTrino.takeRequest())).isEqualTo("DESCRIBE \"nation\"");
        assertThat(readBody(mockTrino.takeRequest())).isEqualTo("SELECT \"nationkey\", \"name\" FROM \"nation\" OFFSET 0 LIMIT 1000");
    }

    @SuppressWarnings("deprecation")
    private static String readBody(RecordedRequest request)
    {
        return request.getUtf8Body();
    }

    private Connection openConnection(String database)
            throws SQLException
    {
        String url = "jdbc:mysql://127.0.0.1:" + mysqlServer.getBoundPort() + "/" + database;
        Properties props = new Properties();
        props.setProperty("user", "trino_tester");
        props.setProperty("password", "");
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");
        return DriverManager.getConnection(url, props);
    }
}
