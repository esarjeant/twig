/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.micromux.cassandra.jdbc;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.micromux.cassandra.jdbc.Utils.BAD_FETCH_DIR;
import static com.micromux.cassandra.jdbc.Utils.BAD_FETCH_SIZE;
import static com.micromux.cassandra.jdbc.Utils.FORWARD_ONLY;
import static com.micromux.cassandra.jdbc.Utils.NO_INTERFACE;

/**
 * <p>
 * The Supported Data types in CQL are as follows:
 * </p>
 * <table>
 * <tr>
 * <th>type</th>
 * <th>java type</th>
 * <th>description</th>
 * </tr>
 * <tr>
 * <td>ascii</td>
 * <td>String</td>
 * <td>ASCII character string</td>
 * </tr>
 * <tr>
 * <td>bigint</td>
 * <td>Long</td>
 * <td>64-bit signed long</td>
 * </tr>
 * <tr>
 * <td>blob</td>
 * <td>ByteBuffer</td>
 * <td>Arbitrary bytes (no validation)</td>
 * </tr>
 * <tr>
 * <td>boolean</td>
 * <td>Boolean</td>
 * <td>true or false</td>
 * </tr>
 * <tr>
 * <td>counter</td>
 * <td>Long</td>
 * <td>Counter column (64-bit long)</td>
 * </tr>
 * <tr>
 * <td>decimal</td>
 * <td>BigDecimal</td>
 * <td>Variable-precision decimal</td>
 * </tr>
 * <tr>
 * <td>double</td>
 * <td>Double</td>
 * <td>64-bit IEEE-754 floating point</td>
 * </tr>
 * <tr>
 * <td>float</td>
 * <td>Float</td>
 * <td>32-bit IEEE-754 floating point</td>
 * </tr>
 * <tr>
 * <td>int</td>
 * <td>Integer</td>
 * <td>32-bit signed int</td>
 * </tr>
 * <tr>
 * <td>text</td>
 * <td>String</td>
 * <td>UTF8 encoded string</td>
 * </tr>
 * <tr>
 * <td>timestamp</td>
 * <td>Date</td>
 * <td>A timestamp</td>
 * </tr>
 * <tr>
 * <td>uuid</td>
 * <td>UUID</td>
 * <td>Type 1 or type 4 UUID</td>
 * </tr>
 * <tr>
 * <td>varchar</td>
 * <td>String</td>
 * <td>UTF8 encoded string</td>
 * </tr>
 * <tr>
 * <td>varint</td>
 * <td>BigInteger</td>
 * <td>Arbitrary-precision integer</td>
 * </tr>
 * </table>
 * 
 */
public class CassandraResultSet extends AbstractResultSet implements ResultSet
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraResultSet.class);

    public static final int DEFAULT_TYPE = ResultSet.TYPE_FORWARD_ONLY;
    public static final int DEFAULT_CONCURRENCY = ResultSet.CONCUR_READ_ONLY;
    public static final int DEFAULT_HOLDABILITY = ResultSet.HOLD_CURSORS_OVER_COMMIT;

    /**
     * The rows iterator.
     */
    private Iterator<Row> rowsIterator;

    /**
     * The current row or {@code null} if there is no row yet.
     */
    private Row row;

    int rowNumber = 0;

    private final CResultSetMetaData meta;

    private final CassandraStatement statement;

    private int resultSetType;

    private int fetchDirection;

    private int fetchSize;

    private boolean wasNull;

    private boolean closed;

    private ColumnDefinitions columnDefinitions;

    /**
     * no argument constructor.
     */
    CassandraResultSet()
    {
        statement = null;
        meta = new CResultSetMetaData();
    }

    /**
     * Instantiates a new cassandra result set from a CqlResult.
     */
    CassandraResultSet(CassandraStatement statement, com.datastax.driver.core.ResultSet resultSet) throws SQLException
    {
        this.statement = statement;
        this.resultSetType = statement.getResultSetType();
        this.fetchDirection = statement.getFetchDirection();
        this.fetchSize = statement.getFetchSize();
        this.columnDefinitions = resultSet.getColumnDefinitions();

        // assign the first row for JDBC to read
        List<Row> rowList = resultSet.all();

        if ((rowList != null) && !rowList.isEmpty()) {
            row = rowList.get(0);
            rowsIterator = rowList.iterator();
        }

        meta = new CResultSetMetaData(resultSet.getColumnDefinitions());

    }

    /**
     * Move to an absolute location in the resultset. This is not supported.
     * @param arg0   Row to move to.
     * @return Value is {@code true} for success.
     * @throws SQLException  Operation is not supported; {@link SQLFeatureNotSupportedException} returns from here.
     */
    @Override
    public boolean absolute(int arg0) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public void afterLast() throws SQLException
    {
        if (resultSetType == TYPE_FORWARD_ONLY) throw new SQLNonTransientException(FORWARD_ONLY);
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public void beforeFirst() throws SQLException
    {
        if (resultSetType == TYPE_FORWARD_ONLY) throw new SQLNonTransientException(FORWARD_ONLY);
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    /**
     * Determine if a row has been selected from the resultset.
     * @return Result is {@code true} if a row is currently selected in the result or {@code false} if it is not.
     * @throws SQLException  Database error requesting the row.
     */
    private boolean hasRow() throws SQLException {
        return (row != null);
    }

    @Override
    public void clearWarnings() throws SQLException
    {
        // not implemented
    }

    @Override
    public void close() throws SQLException
    {
        // not implemented
    }

    @Override
    public int findColumn(String name) throws SQLException
    {
        return columnDefinitions.getIndexOf(name) + 1;
    }

    @Override
    public boolean first() throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public BigDecimal getBigDecimal(int index) throws SQLException
    {
        if (hasRow()) {
            BigDecimal val = row.getDecimal(index - 1);
            wasNull = row.isNull(index - 1);

            return wasNull ? null : val;

        } else {
            throw new SQLDataException("Record Not Found At Index: " + index);
        }
    }

    /** @deprecated */
    @Override
    public BigDecimal getBigDecimal(int index, int scale) throws SQLException
    {
        return getBigDecimal(index);
    }

    @Override
    public BigDecimal getBigDecimal(String name) throws SQLException
    {
        return getBigDecimal(findColumn(name));
    }

    /** @deprecated */
    @Override
    public BigDecimal getBigDecimal(String name, int scale) throws SQLException
    {
        return getBigDecimal(name);
    }

    @Override
    public boolean getBoolean(int index) throws SQLException
    {

        if (hasRow()) {
            Boolean val = row.getBool(index - 1);
            wasNull = row.isNull(index - 1);

            return !wasNull && val;

        } else {
            throw new SQLDataException("Record Not Found At Index: " + index);
        }
    }

    @Override
    public boolean getBoolean(String name) throws SQLException
    {
        return getBoolean(findColumn(name));
    }

    @Override
    public byte getByte(int index) throws SQLException
    {
        if (hasRow()) {

            ByteBuffer val = row.getBytes(index - 1);
            wasNull = row.isNull(index - 1);

            if (val != null) {
                byte[] bytes = val.array();
                return !wasNull && (bytes.length > 0) ? 0x00 : val.get(0);
            } else {
                return 0x00;
            }

        } else {
            throw new SQLDataException("Record Not Found At Index: " + index);
        }
    }

    @Override
    public byte getByte(String name) throws SQLException
    {
        return getByte(findColumn(name));
    }

    @Override
    public byte[] getBytes(int index) throws SQLException
    {
        if (hasRow()) {
            ByteBuffer val = row.getBytes(index - 1);
            wasNull = row.isNull(index - 1) || (null == val);

            return wasNull ? null : val.array();

        } else {
            throw new SQLDataException("Record Not Found At Index: " + index);
        }
    }

    @Override
    public byte[] getBytes(String name) throws SQLException
    {
        return getBytes(findColumn(name));
    }

    @Override
    public int getConcurrency() throws SQLException
    {
        return statement.getResultSetConcurrency();
    }

    @Override
    public Date getDate(int index) throws SQLException
    {
        if (hasRow()) {
            java.util.Date val = row.getDate(index - 1);
            wasNull = row.isNull(index - 1) || (null == val);

            return wasNull ? null : new java.sql.Date(val.getTime());

        } else {
            throw new SQLDataException("Record Not Found At Index: " + index);
        }
    }

    @Override
    public Date getDate(int index, Calendar calendar) throws SQLException
    {
        return getDate(index);
    }

    @Override
    public Date getDate(String name) throws SQLException
    {
        return getDate(findColumn(name));
    }

    @Override
    public Date getDate(String name, Calendar calendar) throws SQLException
    {
        return getDate(name);
    }

    @Override
    public double getDouble(int index) throws SQLException
    {
        if (hasRow()) {
            double val = row.getDouble(index - 1);
            wasNull = row.isNull(index - 1);

            return wasNull ? 0x0d : val;

        } else {
            throw new SQLDataException("Record Not Found At Index: " + index);
        }
    }

    @Override
    public double getDouble(String name) throws SQLException
    {
        return getDouble(findColumn(name));
    }

    @Override
    public int getFetchDirection() throws SQLException
    {
        return fetchDirection;
    }

    @Override
    public int getFetchSize() throws SQLException
    {
        return fetchSize;
    }

    @Override
    public float getFloat(int index) throws SQLException
    {
        if (hasRow()) {
            float val = row.getFloat(index - 1);
            wasNull = row.isNull(index - 1);

            return wasNull ? 0x0f : val;

        } else {
            throw new SQLDataException("Record Not Found At Index: " + index);
        }
    }

    @Override
    public float getFloat(String name) throws SQLException
    {
        return getFloat(findColumn(name));
    }

    @Override
    public int getHoldability() throws SQLException
    {
        return statement.getResultSetHoldability();
    }

    @Override
    public int getInt(int index) throws SQLException
    {
        if (hasRow()) {
            wasNull = row.isNull(index - 1);
            return row.getInt(index - 1);
        } else {
            throw new SQLDataException("Record Not Found At Index: " + index);
        }
    }

    @Override
    public int getInt(String name) throws SQLException
    {
        return getInt(findColumn(name));
    }

    @Override
    public long getLong(int index) throws SQLException
    {
        if (hasRow()) {
            wasNull = row.isNull(index - 1);
            return wasNull ? 0 : row.getLong(index - 1);
        } else {
            throw new SQLDataException("Record Not Found At Index: " + index);
        }
    }

    @Override
    public long getLong(String name) throws SQLException
    {
        return getLong(findColumn(name));
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException
    {
        return meta;
    }

    @Override
    public Object getObject(int index) throws SQLException
    {
        if (hasRow()) {
            wasNull = row.isNull(index - 1);
            return row.getObject(index - 1);
        } else {
            throw new SQLDataException("Record Not Found At Index: " + index);
        }
    }

    @Override
    public Object getObject(String name) throws SQLException
    {
        return getObject(findColumn(name));
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {

        if (type == String.class) {
            return (T)getString(columnIndex);
        } else if (type == Integer.class) {
            return (T)Integer.valueOf(getInt(columnIndex));
        } else if (type == Long.class) {
            return (T)Long.valueOf(getLong(columnIndex));
        } else if (type == Double.class) {
            return (T)Double.valueOf(getDouble(columnIndex));
        } else if (type == Float.class) {
            return (T)Float.valueOf(getFloat(columnIndex));
        } else if (type == java.sql.Date.class) {
            Date date = getDate(columnIndex);
            long millis = date.getTime();
            return (T)(new java.sql.Date(millis));
        } else if (type == java.sql.Time.class) {
            Date date = getDate(columnIndex);
            long millis = date.getTime();
            return (T)(new java.sql.Time(millis));
        } else if (type == java.sql.Timestamp.class) {
            Date date = getDate(columnIndex);
            long millis = date.getTime();
            return (T)(new java.sql.Timestamp(millis));
        } else {
            return (T)getObject(columnIndex);
        }

    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(findColumn(columnLabel), type);
    }

    @Override
    public int getRow() throws SQLException
    {
        return rowNumber;
    }

    @Override
    public RowId getRowId(int index) throws SQLException
    {
        if (hasRow()) {
            wasNull = row.isNull(index - 1);

            if (!wasNull) {
                ByteBuffer byteBuffer = row.getBytes(index - 1);
                return new CassandraRowId(byteBuffer);
            } else {
                return null;
            }
        } else {
            throw new SQLException("ResultSet Not Advanced");
        }
    }

    @Override
    public RowId getRowId(String name) throws SQLException
    {
        return getRowId(findColumn(name));
    }

    /**
     * Cassandra does not natively support short; it is possible this may result in loss of data.
     * @param index  Column identifier to retrieve.
     * @return Column as a short or {@code 0} if it cannot be converted.
     * @throws SQLException  Error requesting this from the database.
     */
    @Override
    public short getShort(int index) throws SQLException
    {
        if (hasRow()) {
            int val = row.getInt(index - 1);
            wasNull = row.isNull(index - 1);

            return wasNull ? 0 : (short)val;

        } else {
            throw new SQLDataException("Record Not Found At Index: " + index);
        }
    }

    @Override
    public short getShort(String name) throws SQLException
    {
        return getShort(findColumn(name));
    }

    @Override
    public Statement getStatement() throws SQLException
    {
        return statement;
    }

    @Override
    public String getString(int index) throws SQLException
    {
        if (hasRow()) {
            wasNull = row.isNull(index - 1);

            if (wasNull) {
                return null;
            } else {

                DataType dataType = row.getColumnDefinitions().getType(index - 1);

                // TODO: should this go elsewhere?
                if ("UUID".equalsIgnoreCase(dataType.getName().toString())) {
                    return row.getUUID(index - 1).toString();
                } else if ("TIMESTAMP".equalsIgnoreCase(dataType.getName().toString())) {
                    return row.getDate(index - 1).toString();
                } else if ("BOOLEAN".equalsIgnoreCase(dataType.getName().toString())) {
                    return Boolean.toString(row.getBool(index - 1));
                } else if ("LONG".equalsIgnoreCase(dataType.getName().toString())) {
                    return Long.toString(row.getLong(index - 1));
                } else if ("INT32".equalsIgnoreCase(dataType.getName().toString())) {
                    return Long.toString(row.getLong(index - 1));
                } else if ("DECIMAL".equalsIgnoreCase(dataType.getName().toString())) {
                    return row.getDecimal(index - 1).toString();
                } else if ("DOUBLE".equalsIgnoreCase(dataType.getName().toString())) {
                    return Double.toString(row.getDouble(index - 1));
                } else if ("INETADDRESS".equalsIgnoreCase(dataType.getName().toString())) {
                    return row.getInet(index - 1).toString();
                } else if ("TIMEUUID".equalsIgnoreCase(dataType.getName().toString())) {
                    return row.getUUID(index - 1).toString();
                } else if ("MAP".equalsIgnoreCase(dataType.getName().toString())) {
                    Map<Object,Object> map = row.getMap(index - 1, Object.class, Object.class);
                    StringBuilder sbout = new StringBuilder();

                    for (Map.Entry<Object,Object> mapEntry : map.entrySet()) {
                        sbout.append(mapEntry.getKey() + "=" + mapEntry.getValue() + ",");
                    }
                    return sbout.toString();
                } else {
                    return row.getString(index - 1);
                }
            }

        } else {
            throw new SQLDataException("Record Not Found At Index: " + index);
        }
    }

    @Override
    public String getString(String name) throws SQLException
    {
        return getString(findColumn(name));
    }

    @Override
    public Time getTime(int index) throws SQLException
    {
        if (hasRow()) {

            wasNull = row.isNull(index - 1);

            if (wasNull) {
                return null;
            } else {
                java.util.Date dt = row.getDate(index - 1);
                return new Time(dt.getTime());
            }
        } else {
            throw new SQLDataException("Record Not Found At Index: " + index);
        }
    }

    @Override
    public Time getTime(int index, Calendar calendar) throws SQLException
    {
        // silently ignore the Calendar argument; its a hint we do not need
        return getTime(index);
    }

    @Override
    public Time getTime(String name) throws SQLException
    {
        return getTime(findColumn(name));
    }

    @Override
    public Time getTime(String name, Calendar calendar) throws SQLException
    {
        // silently ignore the Calendar argument; its a hint we do not need
        return getTime(findColumn(name));
    }

    @Override
    public Timestamp getTimestamp(int index) throws SQLException
    {
        if (hasRow()) {
            wasNull = row.isNull(index - 1);

            if (wasNull) {
                return null;
            } else {
                java.util.Date dt = row.getDate(index - 1);
                return new Timestamp(dt.getTime());
            }
        } else {
            throw new SQLDataException("Record Not Found At Index: " + index);
        }
    }

    @Override
    public Timestamp getTimestamp(int index, Calendar calendar) throws SQLException
    {
        // silently ignore the Calendar argument; its a hint we do not need
        return getTimestamp(index);
    }

    @Override
    public Timestamp getTimestamp(String name) throws SQLException
    {
        return getTimestamp(findColumn(name));
    }

    @Override
    public Timestamp getTimestamp(String name, Calendar calendar) throws SQLException
    {
        // silently ignore the Calendar argument; its a hint we do not need
        return getTimestamp(name);
    }

    @Override
    public Blob getBlob(int index) throws SQLException
    {
        if (hasRow()) {

            wasNull = row.isNull(index - 1);

            if (wasNull) {
                return null;
            } else {
                ByteBuffer bb = row.getBytes(index - 1);
                return new CassandraBlob(bb);
            }
        } else {
            throw new SQLDataException("Record Not Found At Index: " + index);
        }
    }

    @Override
    public Blob getBlob(String name) throws SQLException
    {
        return getBlob(findColumn(name));
    }

    @Override
    public int getType() throws SQLException
    {
        return resultSetType;
    }

    // URL (awaiting some clarifications as to how it is stored in C* ... just a validated Sting in URL format?
    @Override
    public URL getURL(int arg0) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public URL getURL(String arg0) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    // These Methods are planned to be implemented soon; but not right now...
    // Each set of methods has a more detailed set of issues that should be considered fully...


    @Override
    public SQLWarning getWarnings() throws SQLException
    {
        // the rationale is there are no warnings to return in this implementation...
        return null;
    }

    @Override
    public boolean isAfterLast() throws SQLException
    {
        return rowNumber == Integer.MAX_VALUE;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException
    {
        return rowNumber == 0;
    }

    @Override
    public boolean isClosed() throws SQLException
    {
        return closed;
    }

    @Override
    public boolean isFirst() throws SQLException
    {
        return rowNumber == 1;
    }

    @Override
    public boolean isLast() throws SQLException
    {
        return !rowsIterator.hasNext();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        // TODO: should this be implemented?
        return false;
    }

    // Navigation between rows within the returned set of rows
    // Need to use a list iterator so next() needs completely re-thought
    @Override
    public boolean last() throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    /**
     * Move to the next row in the ResultSet. This navigates the internal resultset iterator and returns
     * {@code true} if the next record can be read or {@code false} if there are no more rows.
     * @return {@code true} if the next row can be read or {@code false} if it cannot.
     * @throws SQLException  Fatal error communicating with the resultset.
     */
    @Override
    public synchronized boolean next() throws SQLException
    {
        if ((rowsIterator != null) && rowsIterator.hasNext())
        {
            this.row = rowsIterator.next();
            rowNumber++;
            return true;
        }
        else
        {
            rowNumber = Integer.MAX_VALUE;
            return false;
        }
    }

    @Override
    public boolean previous() throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public boolean relative(int arg0) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException
    {
        if (direction == FETCH_FORWARD || direction == FETCH_REVERSE || direction == FETCH_UNKNOWN)
        {
            if ((getType() == TYPE_FORWARD_ONLY) && (direction != FETCH_FORWARD)) throw new SQLSyntaxErrorException("attempt to set an illegal direction : " + direction);
            fetchDirection = direction;
        }
        throw new SQLSyntaxErrorException(String.format(BAD_FETCH_DIR, direction));
    }

    @Override
    public void setFetchSize(int size) throws SQLException
    {
        if (size < 0) throw new SQLException(String.format(BAD_FETCH_SIZE, size));
        fetchSize = size;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        // TODO: should this be implemented?
        throw new SQLFeatureNotSupportedException(String.format(NO_INTERFACE, iface.getSimpleName()));
    }

    @Override
    public boolean wasNull() throws SQLException
    {
        return wasNull;
    }

    /**
     * RSMD implementation. The metadata returned refers to the column
     * values, not the column names.
     */
    class CResultSetMetaData implements ResultSetMetaData
    {

        private ColumnDefinitions columnDefinitions;

        public CResultSetMetaData() {
            // now what????
        }

        public CResultSetMetaData(ColumnDefinitions columnDefinitions) {
            this.columnDefinitions = columnDefinitions;
        }

        /**
         * Return the Cassandra Cluster Name as the Catalog
         */
        @Override
        public String getCatalogName(int column) throws SQLException
        {
            return columnDefinitions.getKeyspace(column - 1);
        }

        /**
         * Get the JDBC class for this column.
         * @param column   Column identifier.
         * @return JDBC class for the column.
         * @throws SQLException Database error.
         */
        @Override
        public String getColumnClassName(int column) throws SQLException
        {
            return columnDefinitions.getType(column - 1).getName().toString();
        }

        @Override
        public int getColumnCount() throws SQLException
        {
            return (null == columnDefinitions) ? 0 : columnDefinitions.size();
        }

        @Override
        public int getColumnDisplaySize(int column) throws SQLException
        {

            Class colClass  = columnDefinitions.getType(column - 1).asJavaClass();

            if (colClass == String.class) {
                return 50;
            } else if (colClass == ByteBuffer.class) {
                return 75;
            } else if (colClass == Boolean.class) {
                return 4;
            } else {
                return 10;
            }

        }

        public String getColumnLabel(int column) throws SQLException
        {
            return columnDefinitions.getName(column - 1);
        }

        public String getColumnName(int column) throws SQLException
        {
            return columnDefinitions.getName(column - 1);
        }

        public int getColumnType(int column) throws SQLException
        {

            DataType dataType = columnDefinitions.getType(column - 1);
            Class clazz = dataType.asJavaClass();

            if (clazz == String.class) {
                return Types.NVARCHAR;
            }

            if ((clazz == Integer.class) || (clazz == Long.class)) {
                return Types.INTEGER;
            }

            if (clazz == Boolean.class) {
                return Types.BOOLEAN;
            }

            if (clazz == ByteBuffer.class) {
                return Types.BLOB;
            }

            if (dataType.isCollection()) {
                return Types.ARRAY;
            }

            // TODO: JDBC Types missing -- additional needed here

            // default type is Object
            return Types.JAVA_OBJECT;

        }

        /**
         * Spec says "database specific type name"; for Cassandra this means the AbstractType.
         */
        @Override
        public String getColumnTypeName(int column) throws SQLException
        {
            return columnDefinitions.getType(column - 1).asJavaClass().toString();
        }

        @Override
        public int getPrecision(int column) throws SQLException
        {
            // TODO: column size should vary depending on the type
            return 50;
        }

        @Override
        public int getScale(int column) throws SQLException
        {
            int colType = getColumnType(column);

            if ((colType == Types.FLOAT) || (colType == Types.DOUBLE)) {
                return 4;
            } else {
                return 0;
            }
        }

        /**
         * return the DEFAULT current Keyspace as the Schema Name
         */
        @Override
        public String getSchemaName(int column) throws SQLException
        {
            return columnDefinitions.getKeyspace(column - 1);
        }

        @Override
        public String getTableName(int column) throws SQLException
        {
            return columnDefinitions.getTable(column - 1);
        }

        @Override
        public boolean isAutoIncrement(int column) throws SQLException
        {
            // TODO: how should this be implemented?
            return false;
        }


        @Override
        public boolean isCaseSensitive(int column) throws SQLException
        {
            // TODO: why wouldn't this be case sensitive?
            return true;
        }

        @Override
        public boolean isCurrency(int column) throws SQLException
        {
            // TODO: how should this support currency type?
            return false;
        }

        @Override
        public boolean isDefinitelyWritable(int column) throws SQLException
        {
            return isWritable(column);
        }

        /**
         * absence is the equivalent of null in Cassandra
         */
        @Override
        public int isNullable(int column) throws SQLException
        {
            return ResultSetMetaData.columnNullable;
        }

        @Override
        public boolean isReadOnly(int column) throws SQLException
        {
            return column == 0;
        }

        @Override
        public boolean isSearchable(int column) throws SQLException
        {
            return false;
        }

        @Override
        public boolean isSigned(int column) throws SQLException
        {
            // TODO: determine if this is signed
            return false;
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException
        {
            return false;
        }

        @Override
        public boolean isWritable(int column) throws SQLException
        {
            if (columnDefinitions.getType(column - 1) != null) {
                return column > 0;
            } else {
                return false;
            }
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException
        {
            throw new SQLFeatureNotSupportedException(String.format(NO_INTERFACE, iface.getSimpleName()));
        }
        
        
    }
}
