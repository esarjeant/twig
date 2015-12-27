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

import com.datastax.driver.core.BoundStatement;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Set;

class CassandraPreparedStatement extends CassandraStatement implements PreparedStatement
{
    private static final Logger LOG = LoggerFactory.getLogger(CassandraPreparedStatement.class);

    /**
     * Cassandra PreparedStatement - native implementation
     */
    private com.datastax.driver.core.PreparedStatement preparedStatement;

    private com.datastax.driver.core.ResultSet currentResultSet;

    private BoundStatement boundStatement;

    /**
     * Construct a prepared statment for Cassandra JDBC.
     * @param con   Connection to Cassandra
     * @param cql   CQL statement.
     * @param rsType  Type of result set to return.
     * @param rsConcurrency  Concurrency to use.
     * @param rsHoldability  Holdability to use.
     * @throws SQLException  SQL error.
     */
    CassandraPreparedStatement(CassandraConnection con,
                               String cql,
                               int rsType,
                               int rsConcurrency,
                               int rsHoldability
    ) throws SQLException {

        super(con, cql, rsType, rsConcurrency, rsHoldability);

        if (LOG.isTraceEnabled()) LOG.trace("CQL: " + this.cql);

        preparedStatement = con.prepare(cql);
        boundStatement = new BoundStatement(preparedStatement);

    }
    
    String getCql()
    {
        return cql;
    }

    public void close()
    {
        try{
        	connection.removeStatement(this);
        }catch(Exception e){
        	
        }

        connection = null;
    }

    public void addBatch() throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public void setBlob(int parameterIndex, Blob value) throws SQLException {
        checkNotClosed();

        int length = (int)value.length();
        byte[] bytes = value.getBytes(0, length);
        ByteBuffer bb = ByteBuffer.wrap(bytes);

        boundStatement.setBytes(parameterIndex - 1, bb);

    }

    public void clearParameters() throws SQLException
    {
        boundStatement = new BoundStatement(preparedStatement);
    }


    public boolean execute() throws SQLException
    {
        checkNotClosed();

        currentResultSet = connection.execute(boundStatement);

        if ((currentResultSet != null) && (currentResultSet.wasApplied())) {
            updateCount = currentResultSet.all().size() + 1;  // TODO: can INSERT/UPDATE be supported?
        }

        return (currentResultSet != null);

    }

    @Override
    public ResultSet executeQuery() throws SQLException
    {
        checkNotClosed();
        execute();

        if (currentResultSet == null) throw new SQLNonTransientException(Utils.NO_RESULTSET);
        return new CassandraResultSet(this, currentResultSet);

    }

    @Override
    public int executeUpdate() throws SQLException
    {
        checkNotClosed();

        if (execute()) {
            return updateCount;
        } else {
            throw new SQLNonTransientException(Utils.NO_UPDATE_COUNT);
        }
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal decimal) throws SQLException
    {
        boundStatement.setDecimal(parameterIndex - 1, decimal);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean truth) throws SQLException
    {
        boundStatement.setBool(parameterIndex - 1, truth);
    }

    @Override
    public void setByte(int parameterIndex, byte b) throws SQLException
    {
        boundStatement.setBytes(parameterIndex - 1, ByteBufferUtil.bytes((int)b));
    }

    @Override
    public void setBytes(int parameterIndex, byte[] bytes) throws SQLException
    {
        boundStatement.setBytes(parameterIndex - 1, ByteBuffer.wrap(bytes));
    }

    @Override
    public void setDate(int parameterIndex, Date value) throws SQLException
    {
        boundStatement.setDate(parameterIndex - 1, value);
    }

    @Override
    public void setDate(int parameterIndex, Date date, Calendar cal) throws SQLException
    {
        // silently ignore the calendar argument it is not useful for the Cassandra implementation
        setDate(parameterIndex, date);
    }

    @Override
    public void setDouble(int parameterIndex, double decimal) throws SQLException
    {
        boundStatement.setDouble(parameterIndex - 1, decimal);
    }

    @Override
    public void setFloat(int parameterIndex, float decimal) throws SQLException
    {
        boundStatement.setFloat(parameterIndex - 1, decimal);
    }

    @Override
    public void setInt(int parameterIndex, int integer) throws SQLException
    {
        boundStatement.setInt(parameterIndex - 1, integer);
    }

    @Override
    public void setLong(int parameterIndex, long bigint) throws SQLException
    {
        boundStatement.setLong(parameterIndex - 1, bigint);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException
    {
        // treat like a String
        boundStatement.setString(parameterIndex - 1, value);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        setBlob(parameterIndex, new CassandraBlob(inputStream));
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException
    {
        boundStatement.setToNull(parameterIndex - 1);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException
    {
        // silently ignore type and type name for cassandra... just store an empty BB
        setNull(parameterIndex, sqlType);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        boundStatement.setString(parameterIndex - 1, x.toString());
    }

    public void setObject(int parameterIndex, Object object) throws SQLException
    {
        setObject(parameterIndex, object, Types.JAVA_OBJECT, 0);
    }

    public void setObject(int parameterIndex, Object object, int targetSqlType) throws SQLException
    {
        setObject(parameterIndex, object, targetSqlType, 0);
    }

    /**
     * Handle Object (non-native SQL types) via JDBC. This always ignores {@code targetSqlType}
     * and defers the binding to the type of {@code object} specified.
     * @param parameterIndex  Parameter to set as object.
     * @param object          The object to put.
     * @param targetSqlType   (ignored)
     * @param scaleOrLength   (ignored)
     * @throws SQLException  Database error.
     */
    public final void setObject(int parameterIndex, Object object, int targetSqlType, int scaleOrLength) throws SQLException
    {
        checkNotClosed();

        if (object instanceof Set) {
            boundStatement.setSet(parameterIndex - 1, (Set)object);
        } else if (object instanceof Map) {
            boundStatement.setMap(parameterIndex - 1, (Map)object);
        } else if (object instanceof ByteBuffer) {
            boundStatement.setBytes(parameterIndex - 1, (ByteBuffer)object);
        } else if (object instanceof InetAddress) {
            boundStatement.setInet(parameterIndex - 1, (InetAddress)object);
        } else if (object instanceof List) {
            boundStatement.setList(parameterIndex - 1, (List)object);
        } else if (object instanceof Timestamp) {
            long millis = ((Timestamp) object).getTime();
            boundStatement.setDate(parameterIndex - 1, new Date(millis));
        } else if (object instanceof java.sql.Date) {
            long millis = ((java.sql.Date) object).getTime();
            boundStatement.setDate(parameterIndex - 1, new Date(millis));
        } else if (object instanceof String) {
            setString(parameterIndex, (String)object);
        } else if (object instanceof Integer) {
            setInt(parameterIndex, (Integer)object);
        } else if (object instanceof Long) {
            setLong(parameterIndex, (Long)object);
        } else if (object instanceof Boolean) {
            setBoolean(parameterIndex, (Boolean)object);
        } else if (object instanceof Double) {
            setDouble(parameterIndex, (Double)object);
        } else {
            throw new SQLException("Object Type Not Supported: " + object.getClass());
        }
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        setBlob(parameterIndex, new CassandraBlob(inputStream));
    }

    @Override
    public void setRowId(int parameterIndex, RowId value) throws SQLException
    {
        // TODO: how should Cassandra support this?
    }

    @Override
    public void setShort(int parameterIndex, short smallint) throws SQLException
    {
        checkNotClosed();
        boundStatement.setInt(parameterIndex - 1, (int)smallint);
    }

    @Override
    public void setString(int parameterIndex, String value) throws SQLException
    {
        checkNotClosed();
        boundStatement.setString(parameterIndex - 1, value);
    }

    @Override
    public void setTime(int parameterIndex, Time value) throws SQLException
    {
        boundStatement.setDate(parameterIndex - 1, new Date(value.getTime()));
    }

    @Override
    public void setTime(int parameterIndex, Time value, Calendar cal) throws SQLException
    {
        // silently ignore the calendar argument it is not useful for the Cassandra implementation
        boundStatement.setDate(parameterIndex - 1, new Date(value.getTime()));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp value) throws SQLException
    {
        boundStatement.setDate(parameterIndex - 1, new Date(value.getTime()));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp value, Calendar cal) throws SQLException
    {
        // silently ignore the calendar argument it is not useful for the Cassandra implementation
        setTimestamp(parameterIndex, value);
    }

}
