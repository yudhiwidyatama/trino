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

import io.trino.client.ClientSession;
import io.trino.mysqlprotocol.MysqlProtocolConfig;
import io.trino.mysqlprotocol.protocol.MysqlCapability;
import io.trino.mysqlprotocol.session.MysqlSession;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.ZoneId;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class TestMysqlSystemVariableHandler
{
    private final MysqlProtocolConfig config = new MysqlProtocolConfig();
    private final MysqlSystemVariableHandler handler = new MysqlSystemVariableHandler(config);

    @Test
    void testConnectorJBootstrapQueryIntercepted()
    {
        MysqlSession session = createSession();
        String connectorJProbe = "/* mysql-connector-j-8.3.0 */SELECT "
                + "@@session.auto_increment_increment AS auto_increment_increment, "
                + "@@character_set_client AS character_set_client, "
                + "@@max_allowed_packet AS max_allowed_packet, "
                + "@@system_time_zone AS system_time_zone, "
                + "@@time_zone AS time_zone, "
                + "@@transaction_isolation AS transaction_isolation";

        Optional<MysqlSystemVariableHandler.InterceptedResult> result = handler.tryHandle(connectorJProbe, session);
        assertThat(result).isPresent();
        assertThat(result.get().isResultSet()).isTrue();
        assertThat(result.get().columnNames()).containsExactly(
                "auto_increment_increment",
                "character_set_client",
                "max_allowed_packet",
                "system_time_zone",
                "time_zone",
                "transaction_isolation");
        assertThat(result.get().rows()).hasSize(1);
        assertThat(result.get().rows().get(0).get(0)).isEqualTo("1");
        assertThat(result.get().rows().get(0).get(1)).isEqualTo("utf8mb4");
    }

    @Test
    void testSetTimeZoneUpdatesClientSession()
    {
        MysqlSession session = createSession();
        Optional<MysqlSystemVariableHandler.InterceptedResult> result = handler.tryHandle("SET time_zone = 'America/New_York'", session);
        assertThat(result).isPresent();
        assertThat(result.get().isResultSet()).isFalse();
        assertThat(session.clientSession().getTimeZone()).isEqualTo(ZoneId.of("America/New_York"));
    }

    @Test
    void testRegularTrinoSqlPassesThrough()
    {
        MysqlSession session = createSession();
        assertThat(handler.tryHandle("SELECT * FROM tpch.tiny.nation", session)).isEmpty();
        assertThat(handler.tryHandle("SET SESSION query_max_run_time = '5m'", session)).isEmpty();
    }

    @Test
    void testSelectDatabaseReturnsCurrentSchema()
    {
        MysqlSession session = createSession();
        Optional<MysqlSystemVariableHandler.InterceptedResult> result = handler.tryHandle("SELECT DATABASE()", session);
        assertThat(result).isPresent();
        assertThat(result.get().rows().get(0)).containsExactly("tpch.tiny");
    }

    private static MysqlSession createSession()
    {
        ClientSession clientSession = ClientSession.builder()
                .server(URI.create("http://localhost:8080"))
                .user(Optional.of("alice"))
                .source("trino-mysql-protocol")
                .catalog("tpch")
                .schema("tiny")
                .timeZone(ZoneId.of("UTC"))
                .build();
        return new MysqlSession(1, new MysqlCapability(MysqlCapability.DEFAULT_SERVER_CAPABILITIES), clientSession, Optional.empty());
    }
}
