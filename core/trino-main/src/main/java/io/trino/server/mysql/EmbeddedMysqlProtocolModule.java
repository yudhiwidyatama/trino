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
package io.trino.server.mysql;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.http.server.HttpServerInfo;
import io.trino.mysqlprotocol.MysqlProtocolConfig;
import io.trino.mysqlprotocol.MysqlProtocolModule;
import io.trino.mysqlprotocol.MysqlTargetUriProvider;

import java.net.URI;

import static com.google.common.base.Preconditions.checkState;

/**
 * Installs the MySQL wire protocol listener inside the Trino Coordinator JVM in parallel
 * with the coordinator's HTTP(S) REST server.
 */
public class EmbeddedMysqlProtocolModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.install(new MysqlProtocolModule());
    }

    @Provides
    @Singleton
    public MysqlTargetUriProvider createEmbeddedTargetUriProvider(MysqlProtocolConfig config, HttpServerInfo httpServerInfo)
    {
        if (config.getTrinoUri() != null) {
            URI configured = config.getTrinoUri();
            return () -> configured;
        }
        URI target = httpServerInfo.getHttpsUri() != null ? httpServerInfo.getHttpsUri() : httpServerInfo.getHttpUri();
        checkState(target != null, "Coordinator HTTP/HTTPS URI is not available for embedded MySQL protocol listener");
        return () -> target;
    }
}
