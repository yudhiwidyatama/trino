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
package io.trino.mysqlprotocol.server;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-connection MySQL wire packet sequence number (0..255, wrapping around).
 */
public final class MysqlSequenceId
{
    private final AtomicInteger next = new AtomicInteger(0);

    public int current()
    {
        return next.get() & 0xFF;
    }

    public int nextAndIncrement()
    {
        return next.getAndUpdate(v -> (v + 1) & 0xFF);
    }

    public void reset()
    {
        next.set(0);
    }

    public void set(int value)
    {
        next.set(value & 0xFF);
    }
}
