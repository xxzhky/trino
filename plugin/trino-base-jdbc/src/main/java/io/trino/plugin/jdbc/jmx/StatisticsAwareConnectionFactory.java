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
package io.trino.plugin.jdbc.jmx;

import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.ConnectionFactoryDecorator;
import io.trino.plugin.jdbc.ForwardingConnectionFactory;
import io.trino.spi.connector.ConnectorSession;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.sql.Connection;
import java.sql.SQLException;

public class StatisticsAwareConnectionFactory
        extends ForwardingConnectionFactory
{
    private final JdbcApiStats openConnection = new JdbcApiStats();
    private final JdbcApiStats closeConnection = new JdbcApiStats();

    public StatisticsAwareConnectionFactory(ConnectionFactory delegate)
    {
        super(delegate);
    }

    @Override
    public Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        return openConnection.wrap(() -> super.openConnection(session));
    }

    @Override
    public void close()
            throws SQLException
    {
        closeConnection.wrap(super::close);
    }

    @Managed
    @Nested
    public JdbcApiStats getOpenConnection()
    {
        return openConnection;
    }

    @Managed
    @Nested
    public JdbcApiStats getCloseConnection()
    {
        return closeConnection;
    }

    public static class Decorator
            implements ConnectionFactoryDecorator
    {
        @Override
        public int getPriority()
        {
            return STATISTICS_PRIORITY;
        }

        @Override
        public ConnectionFactory decorate(ConnectionFactory delegate)
        {
            return new StatisticsAwareConnectionFactory(delegate);
        }
    }
}
