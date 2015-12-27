package com.micromux.cassandra.jdbc.meta;

import com.micromux.cassandra.jdbc.MetadataResultSets;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class CassandraResultSetMetaData implements ResultSetMetaData {

    private MetadataResultSets metaResults;

    public CassandraResultSetMetaData(MetadataResultSets metaResults) {
        this.metaResults = metaResults;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return metaResults.getColumnCount();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return 0;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        // TODO: should this do anything smarter?
        return 20;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return metaResults.getName(column - 1);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return metaResults.getName(column - 1);
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        // TODO: is this right?
        return metaResults.getStatement().getConnection().getCatalog();
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return 8;
    }

    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return metaResults.getName(column - 1);
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return metaResults.getStatement().getConnection().getCatalog();
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return metaResults.getType();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return null;
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
