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
package io.trino.mysqlproxy;

import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.event.client.EventModule;
import io.airlift.jmx.JmxModule;
import io.airlift.log.LogJmxModule;
import io.airlift.log.Logger;
import io.airlift.node.NodeModule;
import io.airlift.tracetoken.TraceTokenModule;
import io.airlift.tracing.TracingModule;
import org.weakref.jmx.guice.MBeanModule;

import static com.google.common.base.MoreObjects.firstNonNull;

public final class TrinoMysqlProxy
{
    private static final String VERSION = firstNonNull(TrinoMysqlProxy.class.getPackage().getImplementationVersion(), "unknown");

    private TrinoMysqlProxy() {}

    public static void start(Module... extraModules)
    {
        Bootstrap app = new Bootstrap(ImmutableList.<Module>builder()
                .add(new NodeModule())
                .add(new MBeanModule())
                .add(new JmxModule())
                .add(new LogJmxModule())
                .add(new TraceTokenModule())
                .add(new TracingModule("trino-mysql-proxy", VERSION))
                .add(new EventModule())
                .add(new MysqlProxyStandaloneModule())
                .add(extraModules)
                .build());

        Logger log = Logger.get(TrinoMysqlProxy.class);
        try {
            app.initialize();
            log.info("======== MYSQL PROXY STARTED ========");
        }
        catch (Throwable t) {
            log.error(t);
            System.exit(1);
        }
    }

    public static void main(String[] args)
    {
        start();
    }
}
