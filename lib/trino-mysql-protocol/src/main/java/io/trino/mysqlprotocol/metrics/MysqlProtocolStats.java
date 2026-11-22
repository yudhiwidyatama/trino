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
package io.trino.mysqlprotocol.metrics;

import org.weakref.jmx.Managed;

import java.util.concurrent.atomic.AtomicLong;

public class MysqlProtocolStats
{
    private final long startTimeMillis = System.currentTimeMillis();
    private final AtomicLong activeConnections = new AtomicLong();
    private final AtomicLong totalConnections = new AtomicLong();
    private final AtomicLong totalQueries = new AtomicLong();
    private final AtomicLong failedQueries = new AtomicLong();
    private final AtomicLong totalRowsStreamed = new AtomicLong();

    public void connectionOpened()
    {
        activeConnections.incrementAndGet();
        totalConnections.incrementAndGet();
    }

    public void connectionClosed()
    {
        activeConnections.decrementAndGet();
    }

    public void queryStarted()
    {
        totalQueries.incrementAndGet();
    }

    public void queryFailed()
    {
        failedQueries.incrementAndGet();
    }

    public void rowsStreamed(long count)
    {
        totalRowsStreamed.addAndGet(count);
    }

    public long getUptimeSeconds()
    {
        return Math.max(0L, (System.currentTimeMillis() - startTimeMillis) / 1000L);
    }

    @Managed
    public long getActiveConnections()
    {
        return activeConnections.get();
    }

    @Managed
    public long getTotalConnections()
    {
        return totalConnections.get();
    }

    @Managed
    public long getTotalQueries()
    {
        return totalQueries.get();
    }

    @Managed
    public long getFailedQueries()
    {
        return failedQueries.get();
    }

    @Managed
    public long getTotalRowsStreamed()
    {
        return totalRowsStreamed.get();
    }
}
