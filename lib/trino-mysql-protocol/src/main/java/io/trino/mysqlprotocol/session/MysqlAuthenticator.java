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

import com.google.inject.Inject;
import io.trino.client.ClientSession;
import io.trino.mysqlprotocol.MysqlProtocolConfig;
import io.trino.mysqlprotocol.MysqlProtocolConfig.AuthMode;
import io.trino.mysqlprotocol.MysqlProtocolConfig.SingleDbNameTarget;
import io.trino.mysqlprotocol.MysqlTargetUriProvider;
import io.trino.mysqlprotocol.protocol.MysqlCapability;
import io.trino.mysqlprotocol.protocol.packet.HandshakeResponse41Packet;
import io.trino.mysqlprotocol.protocol.packet.HandshakeV10Packet;

import java.time.ZoneId;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class MysqlAuthenticator
{
    private final MysqlProtocolConfig config;
    private final MysqlTargetUriProvider targetUriProvider;

    @Inject
    public MysqlAuthenticator(MysqlProtocolConfig config, MysqlTargetUriProvider targetUriProvider)
    {
        this.config = requireNonNull(config, "config is null");
        this.targetUriProvider = requireNonNull(targetUriProvider, "targetUriProvider is null");
    }

    public String defaultAuthPluginName()
    {
        return config.getAuthMode() == AuthMode.BASIC_CLEAR_PASSWORD
                ? HandshakeV10Packet.MYSQL_CLEAR_PASSWORD
                : HandshakeV10Packet.MYSQL_NATIVE_PASSWORD;
    }

    public boolean requiresClearPasswordSwitch(HandshakeResponse41Packet response)
    {
        if (config.getAuthMode() != AuthMode.BASIC_CLEAR_PASSWORD) {
            return false;
        }
        return !HandshakeV10Packet.MYSQL_CLEAR_PASSWORD.equals(response.authPluginName().orElse(""));
    }

    public MysqlSession authenticate(
            int connectionId,
            int serverCapabilities,
            HandshakeResponse41Packet response,
            Optional<byte[]> clearPasswordBytes,
            boolean isTlsActive)
    {
        if (response.username().isEmpty()) {
            throw new AuthenticationException(1045, "28000", "Access denied for anonymous user");
        }

        Optional<String> basicPassword = Optional.empty();
        if (config.getAuthMode() == AuthMode.BASIC_CLEAR_PASSWORD) {
            if (!isTlsActive) {
                throw new AuthenticationException(1045, "28000", "TLS is required for BASIC_CLEAR_PASSWORD authentication");
            }
            byte[] raw = clearPasswordBytes.orElse(response.authResponse());
            String password = decodeNulTrimmedUtf8(raw);
            basicPassword = Optional.of(password);
        }

        int negotiatedFlags = serverCapabilities & response.capabilities();
        CatalogSchema target = resolveCatalogSchema(response.database(), config);

        ClientSession.Builder sessionBuilder = ClientSession.builder()
                .server(targetUriProvider.getTrinoUri())
                .user(Optional.of(response.username()))
                .source("trino-mysql-protocol")
                .timeZone(ZoneId.systemDefault())
                .clientRequestTimeout(config.getClientRequestTimeout());

        target.catalog().ifPresent(sessionBuilder::catalog);
        target.schema().ifPresent(sessionBuilder::schema);

        return new MysqlSession(connectionId, new MysqlCapability(negotiatedFlags), sessionBuilder.build(), basicPassword);
    }

    public static CatalogSchema resolveCatalogSchema(Optional<String> database, MysqlProtocolConfig config)
    {
        Optional<String> defaultCatalog = Optional.ofNullable(config.getDefaultCatalog()).filter(s -> !s.isEmpty());
        Optional<String> defaultSchema = Optional.ofNullable(config.getDefaultSchema()).filter(s -> !s.isEmpty());

        if (database.isEmpty() || database.get().isBlank()) {
            return new CatalogSchema(defaultCatalog, defaultSchema);
        }

        String db = database.get().trim();
        int dotIndex = db.indexOf('.');
        if (dotIndex > 0 && dotIndex < db.length() - 1) {
            return new CatalogSchema(Optional.of(db.substring(0, dotIndex)), Optional.of(db.substring(dotIndex + 1)));
        }

        SingleDbNameTarget targetMode = config.getSingleDbNameTarget();
        if (targetMode == SingleDbNameTarget.SCHEMA || (targetMode == SingleDbNameTarget.AUTO && defaultCatalog.isPresent())) {
            return new CatalogSchema(defaultCatalog, Optional.of(db));
        }
        return new CatalogSchema(Optional.of(db), defaultSchema);
    }

    private static String decodeNulTrimmedUtf8(byte[] bytes)
    {
        int len = bytes.length;
        while (len > 0 && bytes[len - 1] == 0) {
            len--;
        }
        return new String(bytes, 0, len, UTF_8);
    }

    public record CatalogSchema(Optional<String> catalog, Optional<String> schema) {}

    public static class AuthenticationException
            extends RuntimeException
    {
        private final int errorCode;
        private final String sqlState;

        public AuthenticationException(int errorCode, String sqlState, String message)
        {
            super(message);
            this.errorCode = errorCode;
            this.sqlState = sqlState;
        }

        public int errorCode()
        {
            return errorCode;
        }

        public String sqlState()
        {
            return sqlState;
        }
    }
}
