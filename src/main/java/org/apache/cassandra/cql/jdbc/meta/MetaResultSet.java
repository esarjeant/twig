package org.apache.cassandra.cql.jdbc.meta;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Iterator;
import java.util.List;

public class MetaResultSet implements ResultSet {

    private final List<CassandraRow> rows;

    public MetaResultSet(List<CassandraRow> rows, int position) {
        this.rows = rows;
    }

    @Override
    public ColumnDefinitions getColumnDefinitions() {
        return null;
    }

    @Override
    public boolean isExhausted() {
        return false;
    }

    @Override
    public Row one() {
        return null;
    }

    @Override
    public List<Row> all() {
        return null;
    }

    @Override
    public Iterator<Row> iterator() {
        return null;
    }

    @Override
    public int getAvailableWithoutFetching() {
        return 0;
    }

    @Override
    public boolean isFullyFetched() {
        return false;
    }

    @Override
    public ListenableFuture<Void> fetchMoreResults() {
        return null;
    }

    @Override
    public ExecutionInfo getExecutionInfo() {
        return null;
    }

    @Override
    public List<ExecutionInfo> getAllExecutionInfo() {
        return null;
    }

    @Override
    public boolean wasApplied() {
        return false;
    }
}
