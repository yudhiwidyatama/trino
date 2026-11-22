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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.net.URI;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class MysqlProtocolConfig
{
    public enum AuthMode
    {
        TRUSTED_USER,
        BASIC_CLEAR_PASSWORD
    }

    public enum SingleDbNameTarget
    {
        AUTO,
        CATALOG,
        SCHEMA
    }

    private boolean enabled;
    private String listenHost = "0.0.0.0";
    private int listenPort = 3306;
    private URI trinoUri;
    private String defaultCatalog;
    private String defaultSchema;
    private SingleDbNameTarget singleDbNameTarget = SingleDbNameTarget.AUTO;
    private String serverVersion = "8.0.32-trino";
    private AuthMode authMode = AuthMode.TRUSTED_USER;
    private boolean tlsEnabled;
    private File tlsKeystorePath;
    private String tlsKeystorePassword;
    private File trinoTruststorePath;
    private String trinoTruststorePassword;
    private boolean trinoTlsInsecureSkipVerify;
    private boolean compatInterceptorEnabled = true;
    private DataSize maxAllowedPacket = DataSize.of(64, MEGABYTE);
    private int maxConnections = 1024;
    private int ioThreads = Math.max(2, Runtime.getRuntime().availableProcessors());
    private int queryThreads = 256;
    private Duration idleTimeout = new Duration(8, HOURS);
    private Duration clientRequestTimeout = new Duration(60, SECONDS);

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("mysql-protocol.enabled")
    @ConfigDescription("Enable MySQL wire protocol listener")
    public MysqlProtocolConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    @NotNull
    public String getListenHost()
    {
        return listenHost;
    }

    @Config("mysql-protocol.listen-host")
    @ConfigDescription("Bind host for the MySQL protocol TCP listener")
    public MysqlProtocolConfig setListenHost(String listenHost)
    {
        this.listenHost = listenHost;
        return this;
    }

    @Min(0)
    @Max(65535)
    public int getListenPort()
    {
        return listenPort;
    }

    @Config("mysql-protocol.listen-port")
    @ConfigDescription("Bind port for the MySQL protocol TCP listener (0 for ephemeral port)")
    public MysqlProtocolConfig setListenPort(int listenPort)
    {
        this.listenPort = listenPort;
        return this;
    }

    public URI getTrinoUri()
    {
        return trinoUri;
    }

    @Config("mysql-protocol.trino-uri")
    @ConfigDescription("Target Trino coordinator URI (auto-detected from HttpServerInfo when embedded)")
    public MysqlProtocolConfig setTrinoUri(URI trinoUri)
    {
        this.trinoUri = trinoUri;
        return this;
    }

    public String getDefaultCatalog()
    {
        return defaultCatalog;
    }

    @Config("mysql-protocol.default-catalog")
    @ConfigDescription("Default Trino catalog when client does not specify a database")
    public MysqlProtocolConfig setDefaultCatalog(String defaultCatalog)
    {
        this.defaultCatalog = defaultCatalog;
        return this;
    }

    public String getDefaultSchema()
    {
        return defaultSchema;
    }

    @Config("mysql-protocol.default-schema")
    @ConfigDescription("Default Trino schema when client does not specify a database")
    public MysqlProtocolConfig setDefaultSchema(String defaultSchema)
    {
        this.defaultSchema = defaultSchema;
        return this;
    }

    @NotNull
    public SingleDbNameTarget getSingleDbNameTarget()
    {
        return singleDbNameTarget;
    }

    @Config("mysql-protocol.single-db-name-target")
    @ConfigDescription("Whether an unqualified MySQL database name maps to Trino CATALOG, SCHEMA, or AUTO")
    public MysqlProtocolConfig setSingleDbNameTarget(SingleDbNameTarget singleDbNameTarget)
    {
        this.singleDbNameTarget = singleDbNameTarget;
        return this;
    }

    @NotNull
    public String getServerVersion()
    {
        return serverVersion;
    }

    @Config("mysql-protocol.server-version")
    @ConfigDescription("Server version banner sent in MySQL HandshakeV10")
    public MysqlProtocolConfig setServerVersion(String serverVersion)
    {
        this.serverVersion = serverVersion;
        return this;
    }

    @NotNull
    public AuthMode getAuthMode()
    {
        return authMode;
    }

    @Config("mysql-protocol.auth-mode")
    @ConfigDescription("Authentication mode bridging MySQL login to Trino REST (TRUSTED_USER or BASIC_CLEAR_PASSWORD)")
    public MysqlProtocolConfig setAuthMode(AuthMode authMode)
    {
        this.authMode = authMode;
        return this;
    }

    public boolean isTlsEnabled()
    {
        return tlsEnabled;
    }

    @Config("mysql-protocol.tls.enabled")
    @ConfigDescription("Enable TLS upgrade (CLIENT_SSL) for MySQL wire connections")
    public MysqlProtocolConfig setTlsEnabled(boolean tlsEnabled)
    {
        this.tlsEnabled = tlsEnabled;
        return this;
    }

    public File getTlsKeystorePath()
    {
        return tlsKeystorePath;
    }

    @Config("mysql-protocol.tls.keystore-path")
    public MysqlProtocolConfig setTlsKeystorePath(File tlsKeystorePath)
    {
        this.tlsKeystorePath = tlsKeystorePath;
        return this;
    }

    public String getTlsKeystorePassword()
    {
        return tlsKeystorePassword;
    }

    @Config("mysql-protocol.tls.keystore-password")
    @ConfigSecuritySensitive
    public MysqlProtocolConfig setTlsKeystorePassword(String tlsKeystorePassword)
    {
        this.tlsKeystorePassword = tlsKeystorePassword;
        return this;
    }

    public File getTrinoTruststorePath()
    {
        return trinoTruststorePath;
    }

    @Config("mysql-protocol.trino-tls-truststore-path")
    public MysqlProtocolConfig setTrinoTruststorePath(File trinoTruststorePath)
    {
        this.trinoTruststorePath = trinoTruststorePath;
        return this;
    }

    public String getTrinoTruststorePassword()
    {
        return trinoTruststorePassword;
    }

    @Config("mysql-protocol.trino-tls-truststore-password")
    @ConfigSecuritySensitive
    public MysqlProtocolConfig setTrinoTruststorePassword(String trinoTruststorePassword)
    {
        this.trinoTruststorePassword = trinoTruststorePassword;
        return this;
    }

    public boolean isTrinoTlsInsecureSkipVerify()
    {
        return trinoTlsInsecureSkipVerify;
    }

    @Config("mysql-protocol.trino-tls-insecure-skip-verify")
    public MysqlProtocolConfig setTrinoTlsInsecureSkipVerify(boolean trinoTlsInsecureSkipVerify)
    {
        this.trinoTlsInsecureSkipVerify = trinoTlsInsecureSkipVerify;
        return this;
    }

    public boolean isCompatInterceptorEnabled()
    {
        return compatInterceptorEnabled;
    }

    @Config("mysql-protocol.compat-interceptor.enabled")
    @ConfigDescription("Intercept MySQL client bootstrap variable and SET probes locally")
    public MysqlProtocolConfig setCompatInterceptorEnabled(boolean compatInterceptorEnabled)
    {
        this.compatInterceptorEnabled = compatInterceptorEnabled;
        return this;
    }

    @NotNull
    @MinDataSize("1MB")
    @MaxDataSize("1GB")
    public DataSize getMaxAllowedPacket()
    {
        return maxAllowedPacket;
    }

    @Config("mysql-protocol.max-allowed-packet")
    public MysqlProtocolConfig setMaxAllowedPacket(DataSize maxAllowedPacket)
    {
        this.maxAllowedPacket = maxAllowedPacket;
        return this;
    }

    @Min(1)
    public int getMaxConnections()
    {
        return maxConnections;
    }

    @Config("mysql-protocol.max-connections")
    public MysqlProtocolConfig setMaxConnections(int maxConnections)
    {
        this.maxConnections = maxConnections;
        return this;
    }

    @Min(1)
    public int getIoThreads()
    {
        return ioThreads;
    }

    @Config("mysql-protocol.io-threads")
    public MysqlProtocolConfig setIoThreads(int ioThreads)
    {
        this.ioThreads = ioThreads;
        return this;
    }

    @Min(1)
    public int getQueryThreads()
    {
        return queryThreads;
    }

    @Config("mysql-protocol.query-threads")
    public MysqlProtocolConfig setQueryThreads(int queryThreads)
    {
        this.queryThreads = queryThreads;
        return this;
    }

    @NotNull
    @MinDuration("1s")
    public Duration getIdleTimeout()
    {
        return idleTimeout;
    }

    @Config("mysql-protocol.idle-timeout")
    public MysqlProtocolConfig setIdleTimeout(Duration idleTimeout)
    {
        this.idleTimeout = idleTimeout;
        return this;
    }

    @NotNull
    @MinDuration("1s")
    public Duration getClientRequestTimeout()
    {
        return clientRequestTimeout;
    }

    @Config("mysql-protocol.client-request-timeout")
    public MysqlProtocolConfig setClientRequestTimeout(Duration clientRequestTimeout)
    {
        this.clientRequestTimeout = clientRequestTimeout;
        return this;
    }
}
