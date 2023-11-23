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
package io.trino.plugin.jdbc;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_NON_TRANSIENT_ERROR;
import static io.trino.plugin.jdbc.JdbcWriteSessionProperties.getWriteBatchSize;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class JdbcPageSink
        implements ConnectorPageSink
{
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    private final ConnectorSession session;
    private final JdbcClient jdbcClient;
    private final JdbcOutputTableHandle handle;
    private final ConnectorPageSinkId pageSinkId;
    private final Function<String, String> remoteQueryModifier;
    private final LongWriteFunction pageSinkIdWriteFunction;
    private final boolean includePageSinkIdColumn;

    private final int maxBatchSize;

    private Connection connection;
    private PreparedStatement statement;
    private List<Type> columnTypes;
    private List<WriteFunction> columnWriters;

    private final BatchStats batchStats = new BatchStats();

    public JdbcPageSink(ConnectorSession session, JdbcOutputTableHandle handle, JdbcClient jdbcClient, ConnectorPageSinkId pageSinkId, RemoteQueryModifier remoteQueryModifier)
    {
        this.session = requireNonNull(session, "session is null");
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        this.handle = requireNonNull(handle, "handle is null");
        this.pageSinkId = requireNonNull(pageSinkId, "pageSinkId is null");
        this.remoteQueryModifier = adapt(requireNonNull(remoteQueryModifier, "remoteQueryModifier is null"), session);
        this.pageSinkIdWriteFunction = (LongWriteFunction) jdbcClient.toWriteMapping(session, BaseJdbcClient.TRINO_PAGE_SINK_ID_COLUMN_TYPE).getWriteFunction();
        this.includePageSinkIdColumn = handle.getPageSinkIdColumnName().isPresent();

        // Making batch size configurable allows performance tuning for insert/write-heavy workloads over multiple connections.
        this.maxBatchSize = getWriteBatchSize(session);
    }

    protected void initialize()
    {
        if (!initialized.compareAndSet(false, true)) {
            return;
        }

        try {
            connection = jdbcClient.getConnection(session, handle);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }

        try {
            // According to JDBC javadocs "If a connection is in auto-commit mode, then all its SQL statements will be
            // executed and committed as individual transactions." Notably MySQL and SQL Server respect this which
            // leads to multiple commits when we close the connection leading to slow performance. Explicit commits
            // where needed to ensure that all the submitted statements are committed as a single transaction and
            // performs better.
            connection.setAutoCommit(false);
        }
        catch (SQLException e) {
            closeAllSuppress(e, connection);
            throw new TrinoException(JDBC_ERROR, e);
        }

        columnTypes = handle.getColumnTypes();

        if (handle.getJdbcColumnTypes().isEmpty()) {
            columnWriters = columnTypes.stream()
                    .map(type -> {
                        WriteMapping writeMapping = jdbcClient.toWriteMapping(session, type);
                        WriteFunction writeFunction = writeMapping.getWriteFunction();
                        verify(
                                type.getJavaType() == writeFunction.getJavaType(),
                                "Trino type %s is not compatible with write function %s accepting %s",
                                type,
                                writeFunction,
                                writeFunction.getJavaType());
                        return writeMapping;
                    })
                    .map(WriteMapping::getWriteFunction)
                    .collect(toImmutableList());
        }
        else {
            columnWriters = handle.getJdbcColumnTypes().get().stream()
                    .map(typeHandle -> jdbcClient.toColumnMapping(session, connection, typeHandle)
                            .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "Underlying type is not supported for INSERT: " + typeHandle)))
                    .map(ColumnMapping::getWriteFunction)
                    .collect(toImmutableList());
        }

        try {
            statement = connection.prepareStatement(getSinkSql(jdbcClient, handle, columnWriters));
        }
        catch (TrinoException e) {
            throw closeAllSuppress(e, connection);
        }
        catch (SQLException e) {
            closeAllSuppress(e, connection);
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    protected String getSinkSql(JdbcClient jdbcClient, JdbcOutputTableHandle outputTableHandle, List<WriteFunction> columnWriters)
    {
        return remoteQueryModifier.apply(jdbcClient.buildInsertSql(outputTableHandle, columnWriters));
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        initialize();

        try {
            for (int position = 0; position < page.getPositionCount(); position++) {
                if (includePageSinkIdColumn) {
                    pageSinkIdWriteFunction.set(statement, page.getChannelCount() + 1, pageSinkId.getId());
                }

                for (int channel = 0; channel < page.getChannelCount(); channel++) {
                    appendColumn(page, position, channel);
                }

                batchStats.size.addAndGet(getPositionSize(page, position));

                statement.addBatch();
                if (batchStats.rows.incrementAndGet() >= maxBatchSize) {
                    statement.executeBatch();
                    connection.commit();
                    connection.setAutoCommit(false);
                    batchStats.committed();
                }
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
        return NOT_BLOCKED;
    }

    private long getPositionSize(Page page, int position)
    {
        long size = 0;
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            size += page.getBlock(channel).getEstimatedDataSizeForStats(position);
        }
        return size;
    }

    private void appendColumn(Page page, int position, int channel)
            throws SQLException
    {
        Block block = page.getBlock(channel);
        int parameterIndex = channel + 1;

        WriteFunction writeFunction = columnWriters.get(channel);
        if (block.isNull(position)) {
            writeFunction.setNull(statement, parameterIndex);
            return;
        }

        Type type = columnTypes.get(channel);
        Class<?> javaType = type.getJavaType();
        if (javaType == boolean.class) {
            ((BooleanWriteFunction) writeFunction).set(statement, parameterIndex, type.getBoolean(block, position));
        }
        else if (javaType == long.class) {
            ((LongWriteFunction) writeFunction).set(statement, parameterIndex, type.getLong(block, position));
        }
        else if (javaType == double.class) {
            ((DoubleWriteFunction) writeFunction).set(statement, parameterIndex, type.getDouble(block, position));
        }
        else if (javaType == Slice.class) {
            ((SliceWriteFunction) writeFunction).set(statement, parameterIndex, type.getSlice(block, position));
        }
        else {
            ((ObjectWriteFunction) writeFunction).set(statement, parameterIndex, type.getObject(block, position));
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        if (!initialized.get()) {
            return completedFuture(ImmutableList.of());
        }

        // commit and close
        try (Connection connection = this.connection;
                PreparedStatement statement = this.statement) {
            if (batchStats.rows.get() > 0) {
                statement.executeBatch();
                connection.commit();
            }
        }
        catch (SQLNonTransientException e) {
            throw new TrinoException(JDBC_NON_TRANSIENT_ERROR, e);
        }
        catch (SQLException e) {
            // Convert chained SQLExceptions to suppressed exceptions, so they are visible in the stack trace
            SQLException nextException = e.getNextException();
            while (nextException != null) {
                if (e != nextException) {
                    e.addSuppressed(new Exception("Next SQLException", nextException));
                }
                nextException = nextException.getNextException();
            }
            throw new TrinoException(JDBC_ERROR, "Failed to insert data: " + firstNonNull(e.getMessage(), e), e);
        }
        // pass the successful page sink id
        Slice value = Slices.allocate(Long.BYTES);
        value.setLong(0, pageSinkId.getId());
        return completedFuture(ImmutableList.of(value));
    }

    @SuppressWarnings("unused")
    @Override
    public void abort()
    {
        if (!initialized.get()) {
            return;
        }

        // rollback and close
        try (Connection connection = this.connection;
                PreparedStatement statement = this.statement) {
            // skip rollback if implicitly closed due to an error
            if (!connection.isClosed()) {
                connection.rollback();
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return batchStats.totalSize().get();
    }

    private static Function<String, String> adapt(RemoteQueryModifier modifier, ConnectorSession session)
    {
        return query -> modifier.apply(session, query);
    }

    private record BatchStats(AtomicInteger rows, AtomicLong size, AtomicLong totalSize)
    {
        public BatchStats()
        {
            this(new AtomicInteger(0), new AtomicLong(0), new AtomicLong(0));
        }

        public void committed()
        {
            rows.set(0);
            totalSize.getAndAccumulate(size.getAndSet(0), Long::sum);
        }
    }
}
