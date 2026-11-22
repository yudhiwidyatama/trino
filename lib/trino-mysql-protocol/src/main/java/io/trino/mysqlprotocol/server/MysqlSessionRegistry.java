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

import io.trino.mysqlprotocol.session.MysqlSession;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MysqlSessionRegistry
{
    private final AtomicInteger nextConnectionId = new AtomicInteger(1);
    private final ConcurrentMap<Integer, MysqlSession> sessions = new ConcurrentHashMap<>();

    public int allocateConnectionId()
    {
        return nextConnectionId.getAndIncrement();
    }

    public boolean tryRegister(MysqlSession session, int maxConnections)
    {
        if (sessions.size() >= maxConnections) {
            return false;
        }
        sessions.put(session.connectionId(), session);
        return true;
    }

    public void unregister(int connectionId)
    {
        sessions.remove(connectionId);
    }

    public Optional<MysqlSession> get(int connectionId)
    {
        return Optional.ofNullable(sessions.get(connectionId));
    }

    public int size()
    {
        return sessions.size();
    }

    public void closeAll()
    {
        for (MysqlSession session : sessions.values()) {
            session.closeConnection();
        }
        sessions.clear();
    }
}
