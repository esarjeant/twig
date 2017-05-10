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
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.Row;
import com.google.common.reflect.TypeToken;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.*;
import java.sql.Date;
import java.util.*;

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
 */
class CassandraResultSet extends AbstractResultSet implements ResultSet {

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

    private int rowNumber = 0;

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
    CassandraResultSet() {
        statement = null;
        meta = new CResultSetMetaData();
    }

    /**
     * Instantiates a new cassandra result set from a CqlResult.
     */
    CassandraResultSet(CassandraStatement statement, com.datastax.driver.core.ResultSet resultSet) throws SQLException {
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
     *
     * @param arg0 Row to move to.
     * @return Value is {@code true} for success.
     * @throws SQLException Operation is not supported; {@link SQLFeatureNotSupportedException} returns from here.
     */
    public boolean absolute(int arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void afterLast() throws SQLException {
        if (resultSetType == TYPE_FORWARD_ONLY) throw new SQLNonTransientException(FORWARD_ONLY);
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void beforeFirst() throws SQLException {
        if (resultSetType == TYPE_FORWARD_ONLY) throw new SQLNonTransientException(FORWARD_ONLY);
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    /**
     * Determine if a row has been selected from the resultset.
     *
     * @return Result is {@code true} if a row is currently selected in the result or {@code false} if it is not.
     * @throws SQLException Database error requesting the row.
     */
    private boolean hasRow() throws SQLException {
        return (row != null);
    }

    public void clearWarnings() throws SQLException {
        // not implemented
    }

    public void close() throws SQLException {
        // not implemented
    }

    public int findColumn(String name) throws SQLException {
        return columnDefinitions.getIndexOf(name) + 1;
    }

    public boolean first() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public BigDecimal getBigDecimal(int index) throws SQLException {
        if (hasRow()) {
            BigDecimal val = row.getDecimal(index - 1);
            wasNull = row.isNull(index - 1);

            return wasNull ? null : val;

        } else {
            throw new SQLDataException("Record Not Found At Index: " + index);
        }
    }

    /**
     * @deprecated
     */
    public BigDecimal getBigDecimal(int index, int scale) throws SQLException {
        return getBigDecimal(index);
    }

    public BigDecimal getBigDecimal(String name) throws SQLException {
        return getBigDecimal(findColumn(name));
    }

    /**
     * @deprecated
     */
    public BigDecimal getBigDecimal(String name, int scale) throws SQLException {
        return getBigDecimal(name);
    }

    public boolean getBoolean(int index) throws SQLException {

        if (hasRow()) {
            Boolean val = row.getBool(index - 1);
            wasNull = row.isNull(index - 1);

            return !wasNull && val;

        } else {
            throw new SQLDataException("Record Not Found At Index: " + index);
        }
    }

    public boolean getBoolean(String name) throws SQLException {
        return getBoolean(findColumn(name));
    }

    public byte getByte(int index) throws SQLException {
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

    public byte getByte(String name) throws SQLException {
        return getByte(findColumn(name));
    }

    public byte[] getBytes(int index) throws SQLException {
        if (hasRow()) {
            ByteBuffer val = row.getBytes(index - 1);
            wasNull = row.isNull(index - 1) || (null == val);

            return wasNull ? null : val.array();

        } else {
            throw new SQLDataException("Record Not Found At Index: " + index);
        }
    }

    public byte[] getBytes(String name) throws SQLException {
        return getBytes(findColumn(name));
    }

    public int getConcurrency() throws SQLException {
        return statement.getResultSetConcurrency();
    }

    public Date getDate(int index) throws SQLException {
        if (hasRow()) {
            java.util.Date val = row.getTimestamp(index - 1);
            wasNull = row.isNull(index - 1) || (null == val);

            return wasNull ? null : new java.sql.Date(val.getTime());

        } else {
            throw new SQLDataException("Record Not Found At Index: " + index);
        }
    }

    public Date getDate(int index, Calendar calendar) throws SQLException {
        return getDate(index);
    }

    public Date getDate(String name) throws SQLException {
        return getDate(findColumn(name));
    }

    public Date getDate(String name, Calendar calendar) throws SQLException {
        return getDate(name);
    }

    public double getDouble(int index) throws SQLException {
        if (hasRow()) {
            double val = row.getDouble(index - 1);
            wasNull = row.isNull(index - 1);

            return wasNull ? 0x0d : val;

        } else {
            throw new SQLDataException("Record Not Found At Index: " + index);
        }
    }

    public double getDouble(String name) throws SQLException {
        return getDouble(findColumn(name));
    }

    public int getFetchDirection() throws SQLException {
        return fetchDirection;
    }

    public int getFetchSize() throws SQLException {
        return fetchSize;
    }

    public float getFloat(int index) throws SQLException {
        if (hasRow()) {
            float val = row.getFloat(index - 1);
            wasNull = row.isNull(index - 1);

            return wasNull ? 0x0f : val;

        } else {
            throw new SQLDataException("Record Not Found At Index: " + index);
        }
    }

    public float getFloat(String name) throws SQLException {
        return getFloat(findColumn(name));
    }

    public int getHoldability() throws SQLException {
        return statement.getResultSetHoldability();
    }

    public int getInt(int index) throws SQLException {
        if (hasRow()) {
            wasNull = row.isNull(index - 1);
            return row.getInt(index - 1);
        } else {
            throw new SQLDataException("Record Not Found At Index: " + index);
        }
    }

    public int getInt(String name) throws SQLException {
        return getInt(findColumn(name));
    }

    public long getLong(int index) throws SQLException {
        if (hasRow()) {
            wasNull = row.isNull(index - 1);
            return wasNull ? 0 : row.getLong(index - 1);
        } else {
            throw new SQLDataException("Record Not Found At Index: " + index);
        }
    }

    public long getLong(String name) throws SQLException {
        return getLong(findColumn(name));
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        return meta;
    }

    public Object getObject(int index) throws SQLException {
        if (hasRow()) {
            wasNull = row.isNull(index - 1);

            if (wasNull) {
                return null;
            } else {

                ColumnDefinitions columnDefinitions = row.getColumnDefinitions();

                if (columnDefinitions.getType(index - 1).isCollection()) {
                    return getString(index);
                } else {
                    return row.getObject(index - 1);
                }

            }

        } else {
            throw new SQLDataException("Record Not Found At Index: " + index);
        }
    }

    public Object getObject(String name) throws SQLException {
        return getObject(findColumn(name));
    }

    /**
     * This is some unusual logic; support the Object conversion for JDBC which allows the client to request the
     * type of entity based on a column index lookup. For many of the common types (String, Integer, etc.) the
     * native implementation is used here.
     *
     * @param columnIndex Column entry to request.
     * @param type        Type of object expected.
     * @param <T>         Class of object expected.
     * @return Instance of the object or an exception if this fails.
     * @throws SQLException Exception occurs if the request object does not match the data definition.
     */
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {

        if (type == String.class) {
            return (T) getString(columnIndex);
        } else if (type == Integer.class) {
            return (T) Integer.valueOf(getInt(columnIndex));
        } else if (type == Long.class) {
            return (T) Long.valueOf(getLong(columnIndex));
        } else if (type == Double.class) {
            return (T) Double.valueOf(getDouble(columnIndex));
        } else if (type == Float.class) {
            return (T) Float.valueOf(getFloat(columnIndex));
        } else if (type == java.sql.Date.class) {
            Date date = getDate(columnIndex);
            long millis = date.getTime();
            return (T) (new java.sql.Date(millis));
        } else if (type == java.sql.Time.class) {
            Date date = getDate(columnIndex);
            long millis = date.getTime();
            return (T) (new java.sql.Time(millis));
        } else if (type == java.sql.Timestamp.class) {
            Date date = getDate(columnIndex);
            long millis = date.getTime();
            return (T) (new java.sql.Timestamp(millis));
        } else {
            return (T) getObject(columnIndex);
        }

    }

    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(findColumn(columnLabel), type);
    }

    public int getRow() throws SQLException {
        return rowNumber;
    }

    public RowId getRowId(int index) throws SQLException {
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

    public RowId getRowId(String name) throws SQLException {
        return getRowId(findColumn(name));
    }

    /**
     * Cassandra does not natively support short; it is possible this may result in loss of data.
     *
     * @param index Column identifier to retrieve.
     * @return Column as a short or {@code 0} if it cannot be converted.
     * @throws SQLException Error requesting this from the database.
     */
    public short getShort(int index) throws SQLException {
        if (hasRow()) {
            int val = row.getInt(index - 1);
            wasNull = row.isNull(index - 1);

            return wasNull ? 0 : (short) val;

        } else {
            throw new SQLDataException("Record Not Found At Index: " + index);
        }
    }

    public short getShort(String name) throws SQLException {
        return getShort(findColumn(name));
    }

    public Statement getStatement() throws SQLException {
        return statement;
    }

    public String getString(int index) throws SQLException {
        if (hasRow()) {
            wasNull = row.isNull(index - 1);

            if (wasNull) {
                return null;
            } else {

                DataType dataType = row.getColumnDefinitions().getType(index - 1);

                // TODO: should this go elsewhere?
                if (DataType.Name.UUID.equals(dataType.getName())) {
                    return row.getUUID(index - 1).toString();
                } else if (DataType.Name.TIMESTAMP.equals(dataType.getName())) {
                    java.util.Date dt = row.getTimestamp(index - 1);
                    return Utils.toISODateTime(dt);
                } else if (DataType.Name.DATE.equals(dataType.getName())) {
                    return row.getDate(index - 1).toString();
                } else if (DataType.Name.BOOLEAN.equals(dataType.getName())) {
                    return Boolean.toString(row.getBool(index - 1));
                } else if (DataType.Name.BIGINT.equals(dataType.getName())) {
                    return Long.toString(row.getLong(index - 1));
                } else if (DataType.Name.INT.equals(dataType.getName())) {
                    return Long.toString(row.getInt(index - 1));
                } else if (DataType.Name.DECIMAL.equals(dataType.getName())) {
                    return row.getDecimal(index - 1).toString();
                } else if (DataType.Name.DOUBLE.equals(dataType.getName())) {
                    return Double.toString(row.getDouble(index - 1));
                } else if (DataType.Name.INET.equals(dataType.getName())) {
                    return row.getInet(index - 1).toString();
                } else if (DataType.Name.TIMEUUID.equals(dataType.getName())) {
                    return row.getUUID(index - 1).toString();
                } else if (DataType.Name.SET.equals(dataType.getName())) {

                    List<DataType> typeArguments = dataType.getTypeArguments();
                    TypeToken typeValue = getTypeToken(typeArguments.get(0));

                    Set set = row.getSet(index - 1, typeValue);
                    List<String> strMap = new ArrayList<String>(set.size());

                    for (Object value : set) {
                        strMap.add(String.format("'%s'", value.toString()));
                    }

                    return String.format("[%s]", StringUtils.join(strMap, ","));

                } else if (DataType.Name.LIST.equals(dataType.getName())) {

                    List<DataType> typeArguments = dataType.getTypeArguments();
                    TypeToken typeValue = getTypeToken(typeArguments.get(0));

                    List list = row.getList(index - 1, typeValue);
                    List<String> strMap = new ArrayList<String>(list.size());

                    for (Object value : list) {
                        strMap.add(String.format("'%s'", value.toString()));
                    }

                    return String.format("[%s]", StringUtils.join(strMap, ","));

                } else if (DataType.Name.MAP.equals(dataType.getName())) {

                    List<DataType> typeArguments = dataType.getTypeArguments();
                    TypeToken typeKey = getTypeToken(typeArguments.get(0));
                    TypeToken typeValue = getTypeToken(typeArguments.get(1));

                    Map map = row.getMap(index - 1, typeKey, typeValue);
                    List<String> strMap = new ArrayList<String>(map.size());

                    for (Object key : map.keySet()) {
                        Object value = map.get(key);
                        strMap.add(String.format("'%s'='%s'", key.toString(), value.toString()));
                    }

                    return String.format("[%s]", StringUtils.join(strMap, ","));

                } else {
                    return row.getString(index - 1);
                }
            }

        } else {
            throw new SQLDataException("Record Not Found At Index: " + index);
        }
    }

    /**
     * Convert a DataType to a TypeToken.
     * @param dataType  Data type to convert.
     * @return  Type token
     */
    private TypeToken getTypeToken(DataType dataType) {

        if (StringUtils.equalsIgnoreCase("bigint", dataType.getName().toString())   ||
            StringUtils.equalsIgnoreCase("counter", dataType.getName().toString())  ||
            StringUtils.equalsIgnoreCase("int", dataType.getName().toString())      ||
            StringUtils.equalsIgnoreCase("varint", dataType.getName().toString())      ||
            StringUtils.equalsIgnoreCase("smallint", dataType.getName().toString())) {
            return TypeToken.of(BigInteger.class);
        }

        if (StringUtils.equalsIgnoreCase("boolean", dataType.getName().toString())) {
            return TypeToken.of(Boolean.class);
        }

        if (StringUtils.equalsIgnoreCase("date", dataType.getName().toString())) {
            return TypeToken.of(LocalDate.class);
        }

        if (StringUtils.equalsIgnoreCase("decimal", dataType.getName().toString())) {
            return TypeToken.of(BigDecimal.class);
        }

        if (StringUtils.equalsIgnoreCase("double", dataType.getName().toString())) {
            return TypeToken.of(Double.class);
        }

        if (StringUtils.equalsIgnoreCase("float", dataType.getName().toString())) {
            return TypeToken.of(Float.class);
        }

        if (StringUtils.equalsIgnoreCase("timestamp", dataType.getName().toString())) {
            return TypeToken.of(Date.class);
        }

        if (StringUtils.equalsIgnoreCase("uuid", dataType.getName().toString()) ||
                StringUtils.equalsIgnoreCase("timeuuid", dataType.getName().toString())) {
            return TypeToken.of(UUID.class);
        }

        return TypeToken.of(String.class);

    }

    public String getString(String name) throws SQLException {
        return getString(findColumn(name));
    }

    public Time getTime(int index) throws SQLException {
        if (hasRow()) {

            wasNull = row.isNull(index - 1);

            if (wasNull) {
                return null;
            } else {
                java.util.Date dt = row.getTimestamp(index - 1);
                return new Time(dt.getTime());
            }
        } else {
            throw new SQLDataException("Record Not Found At Index: " + index);
        }
    }

    public Time getTime(int index, Calendar calendar) throws SQLException {
        // silently ignore the Calendar argument; its a hint we do not need
        return getTime(index);
    }

    public Time getTime(String name) throws SQLException {
        return getTime(findColumn(name));
    }

    public Time getTime(String name, Calendar calendar) throws SQLException {
        // silently ignore the Calendar argument; its a hint we do not need
        return getTime(findColumn(name));
    }

    public Timestamp getTimestamp(int index) throws SQLException {
        if (hasRow()) {
            wasNull = row.isNull(index - 1);

            if (wasNull) {
                return null;
            } else {
                java.util.Date dt = row.getTimestamp(index - 1);
                return new Timestamp(dt.getTime());
            }
        } else {
            throw new SQLDataException("Record Not Found At Index: " + index);
        }
    }

    public Timestamp getTimestamp(int index, Calendar calendar) throws SQLException {
        // silently ignore the Calendar argument; its a hint we do not need
        return getTimestamp(index);
    }

    public Timestamp getTimestamp(String name) throws SQLException {
        return getTimestamp(findColumn(name));
    }

    public Timestamp getTimestamp(String name, Calendar calendar) throws SQLException {
        // silently ignore the Calendar argument; its a hint we do not need
        return getTimestamp(name);
    }

    public Blob getBlob(int index) throws SQLException {
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

    public Blob getBlob(String name) throws SQLException {
        return getBlob(findColumn(name));
    }

    public int getType() throws SQLException {
        return resultSetType;
    }

    // URL (awaiting some clarifications as to how it is stored in C* ... just a validated Sting in URL format?
    public URL getURL(int arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public URL getURL(String arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    // These Methods are planned to be implemented soon; but not right now...
    // Each set of methods has a more detailed set of issues that should be considered fully...
    public SQLWarning getWarnings() throws SQLException {
        // the rationale is there are no warnings to return in this implementation...
        return null;
    }

    public boolean isAfterLast() throws SQLException {
        return rowNumber == Integer.MAX_VALUE;
    }

    public boolean isBeforeFirst() throws SQLException {
        return rowNumber == 0;
    }

    public boolean isClosed() throws SQLException {
        return closed;
    }

    public boolean isFirst() throws SQLException {
        return rowNumber == 1;
    }

    public boolean isLast() throws SQLException {
        return !rowsIterator.hasNext();
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        // TODO: should this be implemented?
        return false;
    }

    // Navigation between rows within the returned set of rows
    // Need to use a list iterator so next() needs completely re-thought
    public boolean last() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    /**
     * Move to the next row in the ResultSet. This navigates the internal resultset iterator and returns
     * {@code true} if the next record can be read or {@code false} if there are no more rows.
     *
     * @return {@code true} if the next row can be read or {@code false} if it cannot.
     * @throws SQLException Fatal error communicating with the resultset.
     */
    public synchronized boolean next() throws SQLException {
        if ((rowsIterator != null) && rowsIterator.hasNext()) {
            this.row = rowsIterator.next();
            rowNumber++;
            return true;
        } else {
            rowNumber = Integer.MAX_VALUE;
            return false;
        }
    }

    public boolean previous() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public boolean relative(int arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setFetchDirection(int direction) throws SQLException {
        if (direction == FETCH_FORWARD || direction == FETCH_REVERSE || direction == FETCH_UNKNOWN) {
            if ((getType() == TYPE_FORWARD_ONLY) && (direction != FETCH_FORWARD))
                throw new SQLSyntaxErrorException("attempt to set an illegal direction : " + direction);
            fetchDirection = direction;
        }
        throw new SQLSyntaxErrorException(String.format(BAD_FETCH_DIR, direction));
    }

    public void setFetchSize(int size) throws SQLException {
        if (size < 0) throw new SQLException(String.format(BAD_FETCH_SIZE, size));
        fetchSize = size;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        // TODO: should this be implemented?
        throw new SQLFeatureNotSupportedException(String.format(NO_INTERFACE, iface.getSimpleName()));
    }

    public boolean wasNull() throws SQLException {
        return wasNull;
    }

    /**
     * RSMD implementation. The metadata returned refers to the column
     * values, not the column names.
     */
    class CResultSetMetaData implements ResultSetMetaData {

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
        public String getCatalogName(int column) throws SQLException {
            return columnDefinitions.getKeyspace(column - 1);
        }

        /**
         * Get the JDBC class for this column.
         *
         * @param column Column identifier.
         * @return JDBC class for the column.
         * @throws SQLException Database error.
         */
        public String getColumnClassName(int column) throws SQLException {
            return columnDefinitions.getType(column - 1).getName().toString();
        }

        public int getColumnCount() throws SQLException {
            return (null == columnDefinitions) ? 0 : columnDefinitions.size();
        }

        public int getColumnDisplaySize(int column) throws SQLException {

            DataType dataType = columnDefinitions.getType(column - 1);

            if (dataType.getName().equals(DataType.Name.TEXT)) {
                return 50;
            } else if (dataType.getName().equals(DataType.Name.BLOB)) {
                return 75;
            } else if (dataType.getName().equals(DataType.Name.BOOLEAN)) {
                return 4;
            } else {
                return 10;
            }

        }

        public String getColumnLabel(int column) throws SQLException {
            return columnDefinitions.getName(column - 1);
        }

        public String getColumnName(int column) throws SQLException {
            return columnDefinitions.getName(column - 1);
        }

        public int getColumnType(int column) throws SQLException {

            DataType dataType = columnDefinitions.getType(column - 1);
            DataType.Name dataTypeName = dataType.getName();

            //    varchar
            //    ascii
            //    text
            if (dataTypeName.equals(DataType.Name.ASCII) ||
                    dataTypeName.equals(DataType.Name.VARCHAR) ||
                    dataTypeName.equals(DataType.Name.TEXT)) {
                return Types.VARCHAR;
            }

            // cint
            // bigint
            // counter
            // tinyint
            // smallint
            // varint
            if (dataTypeName.equals(DataType.Name.INT) ||
                    dataTypeName.equals(DataType.Name.VARINT) ||
                    dataTypeName.equals(DataType.Name.BIGINT) ||
                    dataTypeName.equals(DataType.Name.COUNTER) ||
                    dataTypeName.equals(DataType.Name.TINYINT) ||
                    dataTypeName.equals(DataType.Name.SMALLINT)) {
                return Types.INTEGER;
            }

            // cboolean
            if (dataTypeName.equals(DataType.Name.BOOLEAN)) {
                return Types.BOOLEAN;
            }

            // blob
            if (dataTypeName.equals(DataType.Name.BLOB)) {
                return Types.BLOB;
            }

            //    decimal
            if (dataTypeName.equals(DataType.Name.DECIMAL)) {
                return Types.DECIMAL;
            }

            //    cdouble
            if (dataTypeName.equals(DataType.Name.DOUBLE)) {
                return Types.DOUBLE;
            }

            //    cfloat
            if (dataTypeName.equals(DataType.Name.FLOAT)) {
                return Types.FLOAT;
            }

            //    timestamp
            if (dataTypeName.equals(DataType.Name.TIMESTAMP)) {
                return Types.TIMESTAMP;
            }

            //    date
            if (dataTypeName.equals(DataType.Name.DATE)) {
                return Types.DATE;
            }

            //    time
            if (dataTypeName.equals(DataType.Name.TIME)) {
                return Types.TIME;
            }

            if (dataType.isCollection()) {
                return Types.ARRAY;
            }

            // TODO: JDBC Types missing -- additional needed here
            //    inet
            //    uuid
            //    timeuuid

            // default type is Object
            return Types.JAVA_OBJECT;

        }

        /**
         * Spec says "database specific type name"; for Cassandra this means the AbstractType.
         */
        public String getColumnTypeName(int column) throws SQLException {
            return columnDefinitions.getType(column - 1).getName().toString();
        }

        public int getPrecision(int column) throws SQLException {
            // TODO: column size should vary depending on the type
            return 50;
        }

        public int getScale(int column) throws SQLException {
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
        public String getSchemaName(int column) throws SQLException {
            return columnDefinitions.getKeyspace(column - 1);
        }

        public String getTableName(int column) throws SQLException {
            return columnDefinitions.getTable(column - 1);
        }

        public boolean isAutoIncrement(int column) throws SQLException {
            // TODO: how should this be implemented?
            return false;
        }


        public boolean isCaseSensitive(int column) throws SQLException {
            // TODO: why wouldn't this be case sensitive?
            return true;
        }

        public boolean isCurrency(int column) throws SQLException {
            // TODO: how should this support currency type?
            return false;
        }

        public boolean isDefinitelyWritable(int column) throws SQLException {
            return isWritable(column);
        }

        /**
         * absence is the equivalent of null in Cassandra
         */
        public int isNullable(int column) throws SQLException {
            return ResultSetMetaData.columnNullable;
        }

        public boolean isReadOnly(int column) throws SQLException {
            return column == 0;
        }

        public boolean isSearchable(int column) throws SQLException {
            return false;
        }

        public boolean isSigned(int column) throws SQLException {
            // TODO: determine if this is signed
            return false;
        }

        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return false;
        }

        public boolean isWritable(int column) throws SQLException {
            return columnDefinitions.getType(column - 1) != null && column > 0;
        }

        public <T> T unwrap(Class<T> iface) throws SQLException {
            throw new SQLFeatureNotSupportedException(String.format(NO_INTERFACE, iface.getSimpleName()));
        }

    }
}
