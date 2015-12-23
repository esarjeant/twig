package org.apache.cassandra.cql.jdbc.meta;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Encapsulate a {@link CassandraColumn} into a row of one or more columns.
 */
public class CassandraRow {

    private final List<CassandraColumn> columnList = new ArrayList<CassandraColumn>();

    /**
     * Create a <i>row</i> from the columns. This includes both the column meta-data
     * as well as the underlying data.
     * @param columns  Set of columns to create the row from.
     */
    public CassandraRow(CassandraColumn... columns) {
        columnList.addAll(Arrays.asList(columns));
    }

    public CassandraRow(ByteBuffer bytes, List<CassandraColumn> columnList) {
        this.columnList.addAll(columnList);
    }

    public final <T> T getColumnValue(int colId) throws SQLException {

        if (colId < columnList.size()) {
            CassandraColumn<T> cc = columnList.get(colId);
            return cc.getValue();
        } else {
            throw new SQLException("Specified Column Does Not Exist: " + colId);
        }
    }

    public final int findColumnId(String columnName) throws SQLException {

        // SQL columns start at 1
        int colId = 1;

        // try to find a match...
        for (CassandraColumn cc : columnList) {
            if (cc.getName().equalsIgnoreCase(columnName)) {
                return colId;
            }

            colId++;

        }

        throw new SQLException("Column Not Found: " + columnName);

    }

    /**
     * Number of columns defined.
     * @return  Number of columns defined.
     */
    public int getColumnCount() {
        return columnList.size();
    }

    /**
     * Name of the specified column.
     * @param column
     * @return
     */
    public String getColumnName(int column) throws SQLException {

        if ((column >= 0) && (column < columnList.size())) {
            return columnList.get(column).getName();
        } else {
            throw new SQLException("Specified Column Not Found: " + column);
        }
    }
}
