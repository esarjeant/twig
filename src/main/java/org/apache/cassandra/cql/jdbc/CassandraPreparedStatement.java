/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */
package org.apache.cassandra.cql.jdbc;

import static org.apache.cassandra.cql.jdbc.Utils.NO_RESULTSET;
import static org.apache.cassandra.cql.jdbc.Utils.NO_SERVER;
import static org.apache.cassandra.cql.jdbc.Utils.NO_UPDATE_COUNT;
import static org.apache.cassandra.cql.jdbc.Utils.SCHEMA_MISMATCH;
import static org.apache.cassandra.cql.jdbc.Utils.determineCurrentKeyspace;
import static org.apache.cassandra.cql.jdbc.Utils.determineCurrentColumnFamily;
import static org.apache.cassandra.cql.jdbc.Utils.NO_CF;
import static org.apache.cassandra.cql.jdbc.Utils.NO_COMPARATOR;
import static org.apache.cassandra.cql.jdbc.Utils.NO_VALIDATOR;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLNonTransientException;
import java.sql.SQLRecoverableException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLTransientException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.cassandra.thrift.CqlBindValue;
import org.apache.cassandra.thrift.CqlPreparedResult;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlStatementType;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CassandraPreparedStatement extends CassandraStatement implements PreparedStatement
{
    private static final Logger LOG = LoggerFactory.getLogger(CassandraPreparedStatement.class);
    
    /** the statement type (SELECT,INSERT,..) from the parse of the CQL in the server-side */
    private CqlStatementType type;
    
    /** the key token passed back from server-side to identify the prepared statement */
    private int itemId;
    
    /** the count of bound variable markers (?) encountered in the parse o the CQL server-side */    
    private int count;
    
    /** a Map of the current bound values encountered in setXXX methods */    
    private Map<Integer,CqlBindValue> bindValues = new LinkedHashMap<Integer,CqlBindValue>();

    
    CassandraPreparedStatement(CassandraConnection con, String cql) throws SQLException
    {
        super(con, cql);
        if (LOG.isTraceEnabled()) LOG.trace("CQL: "+ this.cql);
        try
        {
            CqlPreparedResult result = con.prepare(cql);
            
            type = result.type;
            itemId = result.itemId;
            count = result.count;
        }
        catch (InvalidRequestException e)
        {
            throw new SQLSyntaxErrorException(e);
        }
         catch (TException e)
        {
            throw new SQLNonTransientConnectionException(e);
        }
    }

    private final void checkIndex(int index) throws SQLException
    {
        if (index > count ) throw new SQLRecoverableException(String.format("the column index : %d is greater than the count of bound variable markers in the CQL: %d", index,count));
        if (index < 1 ) throw new SQLRecoverableException(String.format("the column index must be a positive number : %d", index));
    }
    
    private List<CqlBindValue> getBindValues() throws SQLException
    {
        List<CqlBindValue> values = new ArrayList<CqlBindValue>();
//        System.out.println("bindValues.size() = "+bindValues.size());
//        System.out.println("count             = "+count);
        if (bindValues.size() != count )
            throw new SQLRecoverableException(String.format("the number of bound variables: %d must match the count of bound variable markers in the CQL: %d", bindValues.size(),count));

        for (int i = 1; i <= count ; i++)
        {
            CqlBindValue value = bindValues.get(i);
            if (value==null) throw new SQLRecoverableException(String.format("the bound value for index: %d was not set", i));
           values.add(value);
        }
        return values;
    }

    public void close() throws SQLException
    {
        connection.removeStatement(this);
        try
        {
            connection.release(itemId);
        }
        catch (InvalidRequestException e)
        {
            throw new SQLSyntaxErrorException(e);
        }
         catch (TException e)
        {
            throw new SQLNonTransientConnectionException(e);
        }
        
        connection = null;
    }
        
    private void doExecute() throws SQLException
    {
        if (LOG.isTraceEnabled()) LOG.trace("CQL: "+ cql);
       try
        {
            resetResults();
            CqlResult result = connection.execute(itemId, getBindValues());
            String keyspace = connection.currentKeyspace;

            switch (result.getType())
            {
                case ROWS:
                    currentResultSet = new CResultSet(this, result, keyspace);
                    break;
                case INT:
                    updateCount = result.getNum();
                    break;
                case VOID:
                    updateCount = 0;
                    break;
            }
        }
        catch (InvalidRequestException e)
        {
            throw new SQLSyntaxErrorException(e.getWhy());
        }
        catch (UnavailableException e)
        {
            throw new SQLNonTransientConnectionException(NO_SERVER, e);
        }
        catch (TimedOutException e)
        {
            throw new SQLTransientConnectionException(e.getMessage());
        }
        catch (SchemaDisagreementException e)
        {
            throw new SQLRecoverableException(SCHEMA_MISMATCH);
        }
        catch (TException e)
        {
            throw new SQLNonTransientConnectionException(e.getMessage());
        }
    }
    
    public void addBatch() throws SQLException
    {
        // TODO Auto-generated method stub
        
    }


    public void clearParameters() throws SQLException
    {
        checkNotClosed();
        bindValues.clear();
    }


    public boolean execute() throws SQLException
    {
        checkNotClosed();
        doExecute();
        return !(currentResultSet == null);
    }


    public ResultSet executeQuery() throws SQLException
    {
        checkNotClosed();
        doExecute();
        if (currentResultSet == null) throw new SQLNonTransientException(NO_RESULTSET);
        return currentResultSet;
    }


    public int executeUpdate() throws SQLException
    {
        checkNotClosed();
        doExecute();
        if (currentResultSet != null) throw new SQLNonTransientException(NO_UPDATE_COUNT);
        return updateCount;
     }


    public ResultSetMetaData getMetaData() throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }


    public ParameterMetaData getParameterMetaData() throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }


    public void setBigDecimal(int parameterIndex, BigDecimal decimal) throws SQLException
    {
        checkNotClosed();
        checkIndex(parameterIndex);
        bindValues.put(parameterIndex, new CqlBindValue(false, ByteBufferUtil.bytes(decimal.toPlainString())));
    }


    public void setBoolean(int parameterIndex, boolean truth) throws SQLException
    {
        checkNotClosed();
        checkIndex(parameterIndex);
        bindValues.put(parameterIndex, new CqlBindValue(false, ByteBufferUtil.bytes(Boolean.valueOf(truth).toString())));
    }


    public void setByte(int parameterIndex, byte b) throws SQLException
    {
        checkNotClosed();
        checkIndex(parameterIndex);
        bindValues.put(parameterIndex, new CqlBindValue(false, ByteBufferUtil.bytes(Byte.valueOf(b).toString())));
    }


    public void setBytes(int parameterIndex, byte[] bytes) throws SQLException
    {
        checkNotClosed();
        checkIndex(parameterIndex);
        bindValues.put(parameterIndex, new CqlBindValue(true, ByteBuffer.wrap(bytes)));
    }


    public void setDate(int parameterIndex, Date value) throws SQLException
    {
        checkNotClosed();
        checkIndex(parameterIndex);
        // date type data is handled as an 8 byte Long value of milliseconds since the epoch
        String millis = Long.valueOf(value.getTime()).toString();
        bindValues.put(parameterIndex, new CqlBindValue(false, ByteBufferUtil.bytes(millis)));
    }


    public void setDate(int parameterIndex, Date date, Calendar cal) throws SQLException
    {
        // silently ignore the calendar argument it is not useful for the Cassandra implementation
        setDate(parameterIndex,date);
    }


    public void setDouble(int parameterIndex, double decimal) throws SQLException
    {
        checkNotClosed();
        checkIndex(parameterIndex);
        bindValues.put(parameterIndex, new CqlBindValue(false, ByteBufferUtil.bytes(Double.valueOf(decimal).toString())));
    }


    public void setFloat(int parameterIndex, float decimal) throws SQLException
    {
        checkNotClosed();
        checkIndex(parameterIndex);
        bindValues.put(parameterIndex, new CqlBindValue(false, ByteBufferUtil.bytes(Float.valueOf(decimal).toString())));
    }


    public void setInt(int parameterIndex, int integer) throws SQLException
    {
        checkNotClosed();
        checkIndex(parameterIndex);
        bindValues.put(parameterIndex, new CqlBindValue(false, ByteBufferUtil.bytes(Integer.valueOf(integer).toString())));
    }


    public void setLong(int parameterIndex, long bigint) throws SQLException
    {
        checkNotClosed();
        checkIndex(parameterIndex);
        bindValues.put(parameterIndex, new CqlBindValue(false, ByteBufferUtil.bytes(Long.valueOf(bigint).toString())));
    }


    public void setNString(int parameterIndex, String value) throws SQLException
    {
        // treat like a String
        setString(parameterIndex,value);
    }


    public void setNull(int parameterIndex, int sqlType) throws SQLException
    {
        checkNotClosed();
        checkIndex(parameterIndex);
        // silently ignore type for cassandra... just store an empty BB
        bindValues.put(parameterIndex, new CqlBindValue(false, ByteBufferUtil.EMPTY_BYTE_BUFFER));
    }


    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException
    {
        // silently ignore type and type name for cassandra... just store an empty BB
        setNull(parameterIndex,sqlType);
    }


    public void setObject(int parameterIndex, Object object) throws SQLException
    {
        checkNotClosed();
        checkIndex(parameterIndex);
        
        byte[] bytes = null;
        try
        {
            // Serialize to a byte array
            ByteArrayOutputStream bos = new ByteArrayOutputStream() ;
            ObjectOutput out = new ObjectOutputStream(bos) ;
            out.writeObject(object);
            out.close();

            // Get the bytes of the serialized object
            bytes = bos.toByteArray();
        } 
        catch (IOException e)
        {
            throw new SQLNonTransientException("Problem serializing the object", e);
        }
        
        bindValues.put(parameterIndex, new CqlBindValue(true, ByteBuffer.wrap(bytes)));        
    }


    public void setRowId(int parameterIndex, RowId value) throws SQLException
    {
        checkNotClosed();
        checkIndex(parameterIndex);
        bindValues.put(parameterIndex, new CqlBindValue(true, ByteBuffer.wrap(value.getBytes())));
    }


    public void setShort(int parameterIndex, short smallint) throws SQLException
    {
        checkNotClosed();
        checkIndex(parameterIndex);
        bindValues.put(parameterIndex, new CqlBindValue(false, ByteBufferUtil.bytes(Short.valueOf(smallint).toString())));
    }


    public void setString(int parameterIndex, String value) throws SQLException
    {
        checkNotClosed();
        checkIndex(parameterIndex);
        bindValues.put(parameterIndex, new CqlBindValue(false, ByteBufferUtil.bytes(value)));
    }


    public void setTime(int parameterIndex, Time value) throws SQLException
    {
        checkNotClosed();
        checkIndex(parameterIndex);
        // time type data is handled as an 8 byte Long value of milliseconds since the epoch
        String millis = Long.valueOf(value.getTime()).toString();
        bindValues.put(parameterIndex, new CqlBindValue(false, ByteBufferUtil.bytes(millis)));
    }


    public void setTime(int parameterIndex, Time value, Calendar cal) throws SQLException
    {
        // silently ignore the calendar argument it is not useful for the Cassandra implementation
        setTime(parameterIndex,value);
    }


    public void setTimestamp(int parameterIndex, Timestamp value) throws SQLException
    {
        checkNotClosed();
        checkIndex(parameterIndex);
        // timestamp type data is handled as an 8 byte Long value of milliseconds since the epoch
        String millis = Long.valueOf(value.getTime()).toString();
        bindValues.put(parameterIndex, new CqlBindValue(false, ByteBufferUtil.bytes(millis)));
    }


    public void setTimestamp(int parameterIndex, Timestamp value, Calendar cal) throws SQLException
    {
        // silently ignore the calendar argument it is not useful for the Cassandra implementation
        setTimestamp(parameterIndex,value);
    }


    public void setURL(int parameterIndex, URL value) throws SQLException
    {
        checkNotClosed();
        checkIndex(parameterIndex);
        // URl type data is handled as an string
        String url = value.toString();
        bindValues.put(parameterIndex, new CqlBindValue(false, ByteBufferUtil.bytes(url)));
    }
}
