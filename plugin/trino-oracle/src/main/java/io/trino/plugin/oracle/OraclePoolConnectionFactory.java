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
package io.trino.plugin.oracle;

import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.jdbc.datasource.OpenTelemetryDataSource;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.spi.connector.ConnectorSession;
import oracle.jdbc.pool.OracleDataSource;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

import javax.sql.DataSource;

import java.lang.ref.SoftReference;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Properties;

import static java.lang.Math.toIntExact;
import static java.util.concurrent.TimeUnit.SECONDS;

public class OraclePoolConnectionFactory
        implements ConnectionFactory
{
    private final DataSource dataSource;
    private final PoolDataSource innerSource;
    public boolean alarm;
    ArrayList<ConnInfo> list = new ArrayList<>();
    public int counter;

    public OraclePoolConnectionFactory(
            String connectionUrl,
            Properties connectionProperties,
            CredentialProvider credentialProvider,
            int connectionPoolMinSize,
            int connectionPoolMaxSize,
            Duration inactiveConnectionTimeout,
            OpenTelemetry openTelemetry)
            throws SQLException
    {
        PoolDataSource dataSource = PoolDataSourceFactory.getPoolDataSource();

        //Setting connection properties of the data source
        dataSource.setConnectionFactoryClassName(OracleDataSource.class.getName());
        dataSource.setURL(connectionUrl);

        //Setting pool properties
        dataSource.setInitialPoolSize(connectionPoolMinSize);
        dataSource.setMinPoolSize(connectionPoolMinSize);
        dataSource.setMaxPoolSize(connectionPoolMaxSize);
        dataSource.setValidateConnectionOnBorrow(true);
        dataSource.setConnectionProperties(connectionProperties);
        // dataSource.setAbandonedConnectionTimeout(2000);
        // dataSource.setConnectionHarvestMaxCount(5);
        // dataSource.setConnectionHarvestTriggerCount(10);
        dataSource.setInactiveConnectionTimeout(toIntExact(inactiveConnectionTimeout.roundTo(SECONDS)));
        credentialProvider.getConnectionUser(Optional.empty())
                .ifPresent(user -> {
                    try {
                        dataSource.setUser(user);
                    }
                    catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
        credentialProvider.getConnectionPassword(Optional.empty())
                .ifPresent(password -> {
                    try {
                        dataSource.setPassword(password);
                    }
                    catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
        this.innerSource = dataSource;
        this.dataSource = dataSource;
        var thisDataSource = new OpenTelemetryDataSource(dataSource, openTelemetry);
    }

    static class ConnInfo
    {
        StackTraceElement[] stackElements;
        String queryId;
        SoftReference<Connection> conn;
    }

    @Override
    public Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        Logger log = Logger.get(OraclePoolConnectionFactory.class);
        try {
            Connection connection = null;
            synchronized (this) {
                connection = dataSource.getConnection();
                connection.setAutoCommit(true);
                counter++;
                if (counter == 35) {
                    log.info(" TRACE ALARM - counter 35 ");
                    // alarm = true;
                }
            }
            // Oracle's pool doesn't reset autocommit state of connections when reusing them so we explicitly enable
            // autocommit by default to match the JDBC specification.

            boolean found = false;
            synchronized (list) {
                outer:
                for (int i1 = 0; i1 < list.size(); i1++) {
                    Connection oneConn = list.get(i1).conn.get();
                    /*while ((oneConn == null) || oneConn.isClosed()) {
                        list.remove(i1);
                        if (i1 >= list.size()) {
                            break outer;
                        }
                        oneConn = list.get(i1).conn.get();
                    }*/

                    if (oneConn != null && oneConn.equals(connection)) {
                        found = true;
                    }
                }
            }
            if (!found) {
                ConnInfo connInfo = new ConnInfo();
                connInfo.conn = new SoftReference<>(connection);
                connInfo.queryId = session.getQueryId();
                connInfo.stackElements = Thread.currentThread().getStackTrace();
                synchronized (list) {
                    list.add(connInfo);
                }
            }
            boolean dumpConns = false;
            if (alarm) {
                log.info("alarmed openconnection for " + this.toString());
                dumpConns = true;
            }
            /*
            if (list.size() == 10) {
                log.info("10 connections, sampling; connection list : ");
                dumpConns = true;
            }*/
            //if (innerSource.getBorrowedConnectionsCount() == 29) {
            //    dumpConns = true;
            //}
            // dumpConns = false; // disable tracing for now

            if (dumpConns) {
                log.info("dumping connections for " + this.toString());
                synchronized (list) {
                    for (ConnInfo info : list) {
                        Connection bConn = info.conn.get();
                        if (bConn == null) {
                            continue;
                        }
                        String prefix = "";
                        if (bConn.isClosed()) {
                            prefix = "(CLOSED)";
                        }

                        log.info(prefix + "queryId : " + info.queryId + " class : " + bConn.getClass().toString());
                        for (StackTraceElement el : info.stackElements) {
                            var s1 = el.toString();
                            if (s1.startsWith("io.trino.plugin.jdbc.BaseJdbcClient")) {
                                log.info("-- " + el.toString());
                            }
                        }
                    }
                }
            }
            log.info("succeed getting connection " + connection.toString() + " for queryId " + session.getQueryId() + " connlistsize = " +
                    "" + list.size() +
                    " borrowed = " + innerSource.getBorrowedConnectionsCount() +
                    " available = " + innerSource.getAvailableConnectionsCount() + " source " + this.toString() + " ctr " + counter);
            if (innerSource.getBorrowedConnectionsCount() > 11) {
                alarm = true;
            }

            return connection;
        }
        catch (SQLException ex) {
            log.error("failed opening connection for queryId " + session.getQueryId() + " borrowed = " + innerSource.getBorrowedConnectionsCount() +
                    " available = " + innerSource.getAvailableConnectionsCount() + " reason = "
                    + ex.getMessage() + " source " + this.toString());
            throw ex;
        }
    }
}
