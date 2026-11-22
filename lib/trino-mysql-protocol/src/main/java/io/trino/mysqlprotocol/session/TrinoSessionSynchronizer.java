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

import io.trino.client.ClientSelectedRole;
import io.trino.client.ClientSession;
import io.trino.client.StatementClient;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Applies Trino REST response headers (X-Trino-Set-Catalog, X-Trino-Set-Schema, X-Trino-Set-Session,
 * transaction IDs, roles, prepared statements, etc.) back onto the session's immutable {@link ClientSession}.
 */
public final class TrinoSessionSynchronizer
{
    private TrinoSessionSynchronizer() {}

    public static void synchronize(MysqlSession session, StatementClient client)
    {
        ClientSession current = session.clientSession();
        ClientSession.Builder builder = ClientSession.builder(current);
        boolean changed = false;

        if (client.getSetCatalog().isPresent()) {
            builder.catalog(client.getSetCatalog().get());
            changed = true;
        }
        if (client.getSetSchema().isPresent()) {
            builder.schema(client.getSetSchema().get());
            changed = true;
        }
        if (client.getSetPath().isPresent()) {
            builder.path(client.getSetPath().get());
            changed = true;
        }
        if (client.getSetAuthorizationUser().isPresent()) {
            builder.authorizationUser(client.getSetAuthorizationUser());
            changed = true;
        }
        else if (client.isResetAuthorizationUser()) {
            builder.authorizationUser(Optional.empty());
            changed = true;
        }

        if (!client.getSetSessionProperties().isEmpty() || !client.getResetSessionProperties().isEmpty()) {
            Map<String, String> properties = new HashMap<>(current.getProperties());
            properties.putAll(client.getSetSessionProperties());
            client.getResetSessionProperties().forEach(properties::remove);
            builder.properties(properties);
            changed = true;
        }

        if (!client.getSetRoles().isEmpty()) {
            Map<String, ClientSelectedRole> roles = new HashMap<>(current.getRoles());
            roles.putAll(client.getSetRoles());
            builder.roles(roles);
            changed = true;
        }

        if (!client.getAddedPreparedStatements().isEmpty() || !client.getDeallocatedPreparedStatements().isEmpty()) {
            Map<String, String> prepared = new HashMap<>(current.getPreparedStatements());
            prepared.putAll(client.getAddedPreparedStatements());
            client.getDeallocatedPreparedStatements().forEach(prepared::remove);
            builder.preparedStatements(prepared);
            changed = true;
        }

        if (client.getStartedTransactionId() != null) {
            builder.transactionId(client.getStartedTransactionId());
            changed = true;
        }
        else if (client.isClearTransactionId()) {
            builder.transactionId(null);
            changed = true;
        }

        if (changed) {
            session.setClientSession(builder.build());
        }
    }
}
