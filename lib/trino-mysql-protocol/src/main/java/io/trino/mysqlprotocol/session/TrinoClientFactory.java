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
import io.trino.client.OkHttpUtil;
import io.trino.mysqlprotocol.MysqlProtocolConfig;
import jakarta.annotation.PreDestroy;
import okhttp3.OkHttpClient;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * Shared OkHttpClient factory configured for outbound Trino REST API calls.
 */
public class TrinoClientFactory
{
    private final OkHttpClient baseClient;

    @Inject
    public TrinoClientFactory(MysqlProtocolConfig config)
    {
        requireNonNull(config, "config is null");
        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        OkHttpUtil.setupTimeouts(builder, 30, TimeUnit.SECONDS);
        if (config.isTrinoTlsInsecureSkipVerify()) {
            OkHttpUtil.setupInsecureSsl(builder);
        }
        else if (config.getTrinoTruststorePath() != null) {
            OkHttpUtil.setupSsl(
                    builder,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    false,
                    Optional.of(config.getTrinoTruststorePath().getAbsolutePath()),
                    Optional.ofNullable(config.getTrinoTruststorePassword()),
                    Optional.empty(),
                    false);
        }
        this.baseClient = builder.build();
    }

    public OkHttpClient clientFor(MysqlSession session)
    {
        if (session.basicAuthPassword().isEmpty()) {
            return baseClient;
        }
        String user = session.clientSession().getUser().orElse("");
        String password = session.basicAuthPassword().get();
        OkHttpClient.Builder derived = baseClient.newBuilder();
        derived.addInterceptor(OkHttpUtil.basicAuth(user, password));
        return derived.build();
    }

    @PreDestroy
    public void shutdown()
    {
        baseClient.dispatcher().executorService().shutdown();
        baseClient.connectionPool().evictAll();
    }
}
