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
package com.micromux.cassandra.jdbc;

import com.datastax.driver.core.ConsistencyLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * Cassandra statement: implementation class for {@link PreparedStatement}.
 */
class CassandraStatement extends AbstractStatement implements CassandraStatementExtras, Comparable<Object>, Statement
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraStatement.class);

    /**
     * The connection.
     */
    protected CassandraConnection connection;

    /**
     * The cql.
     */
    protected String cql;

    protected int fetchDirection = ResultSet.FETCH_FORWARD;

    protected int fetchSize = 0;

    protected int maxFieldSize = 0;

    protected int maxRows = 0;

    protected int resultSetType = CassandraResultSet.DEFAULT_TYPE;

    protected int resultSetConcurrency = CassandraResultSet.DEFAULT_CONCURRENCY;

    protected int resultSetHoldability = CassandraResultSet.DEFAULT_HOLDABILITY;

    protected ResultSet currentResultSet = null;

    protected int updateCount = -1;

    protected boolean escapeProcessing = true;
    
    protected ConsistencyLevel consistencyLevel;

    CassandraStatement(CassandraConnection con) throws SQLException
    {
        this(con, null, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    CassandraStatement(CassandraConnection con, String cql) throws SQLException
    {
        this(con, cql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    CassandraStatement(CassandraConnection con, String cql, int resultSetType, int resultSetConcurrency) throws SQLException
    {
        this(con, cql, resultSetType, resultSetConcurrency, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    CassandraStatement(CassandraConnection con, String cql, int resultSetType, int resultSetConcurrency,
                       int resultSetHoldability) throws SQLException
    {
        this.connection = con;
        this.cql = cql;
        this.consistencyLevel = con.defaultConsistencyLevel;

        if (!(resultSetType == ResultSet.TYPE_FORWARD_ONLY
              || resultSetType == ResultSet.TYPE_SCROLL_INSENSITIVE
              || resultSetType == ResultSet.TYPE_SCROLL_SENSITIVE)) throw new SQLSyntaxErrorException(Utils.BAD_TYPE_RSET);
        this.resultSetType = resultSetType;

        if (!(resultSetConcurrency == ResultSet.CONCUR_READ_ONLY
              || resultSetConcurrency == ResultSet.CONCUR_UPDATABLE)) throw new SQLSyntaxErrorException(Utils.BAD_TYPE_RSET);
        this.resultSetConcurrency = resultSetConcurrency;


        if (!(resultSetHoldability == ResultSet.HOLD_CURSORS_OVER_COMMIT
              || resultSetHoldability == ResultSet.CLOSE_CURSORS_AT_COMMIT))
            throw new SQLSyntaxErrorException(Utils.BAD_HOLD_RSET);
        this.resultSetHoldability = resultSetHoldability;
    }

    public void addBatch(String arg0) throws SQLException
    {
        checkNotClosed();
        throw new SQLFeatureNotSupportedException(Utils.NO_BATCH);
    }

//    /**
//     * Take a look at the CQL and determine the name of the columngroup (table)
//     * being queried. Cassandra does not support joins, so the FROM clause
//     * will be the name of the table.
//     * @return   Name of the table queried as part of this statement.
//     */
//    protected String getTableName() {
//
//        String tableName = "";
//
//        if (cql != null) {
//
//            StringTokenizer st = new StringTokenizer(cql, " ", false);
//            String field = "";
//            boolean nextFieldTable = false;
//
//            while ((field = st.nextToken()) != null) {
//
//                if (nextFieldTable) {
//                    tableName = field;
//                    break;
//                }
//
//                nextFieldTable = "FROM".equalsIgnoreCase(field);
//
//            }
//
//        }
//
//        return tableName;
//
//    }

    /**
     * Check the connection and return a fatal SQL Exception if it is no longer available.
     * @throws SQLException  SQL exception if the connection has been closed.
     */
    protected final void checkNotClosed() throws SQLException
    {
        if ((null == connection) || (connection.isClosed())) {
            throw new SQLRecoverableException(Utils.WAS_CLOSED_STMT);
        }
    }

    @Override
    public void clearBatch() throws SQLException
    {
        checkNotClosed();
        throw new SQLFeatureNotSupportedException(Utils.NO_BATCH);
    }

    @Override
    public void clearWarnings() throws SQLException
    {
        // This implementation does not support the collection of warnings so clearing is a no-op
        // but it is still an exception to call this on a closed connection.
        checkNotClosed();
    }

    @Override
    public void close() throws SQLException
    {
        connection.removeStatement(this);
        connection = null;
        cql = null;
    }


//    private void doXExecute(String cql) throws SQLException
//    {
//        try
//        {
//            if (logger.isTraceEnabled()) logger.trace("CQL: "+ cql);
//
//            resetResults();
//            connection.execute(cql, consistencyLevel);
//
////            switch (rSet.())
////            {
////                case ROWS:
////                    currentResultSet = new CassandraResultSet(this, rSet);
////                    break;
////                case INT:
////                    updateCount = rSet.getNum();
////                    break;
////                case VOID:
////                    updateCount = 0;
////                    break;
////            }
//        }
//        catch (InvalidRequestException e)
//        {
//            throw new SQLSyntaxErrorException(e.getWhy()+"\n'"+cql+"'",e);
//        }
//        catch (UnavailableException e)
//        {
//            throw new SQLNonTransientConnectionException(NO_SERVER, e);
//        }
//        catch (TimedOutException e)
//        {
//            throw new SQLTransientConnectionException(e);
//        }
//        catch (SchemaDisagreementException e)
//        {
//            throw new SQLRecoverableException(SCHEMA_MISMATCH);
//        }
//        catch (TException e)
//        {
//        	try{
//        		// Try to close the connection in order to force client to reconnect
//        		connection.close();
//        	}catch(Exception e1){
//
//        	}
//            throw new SQLNonTransientConnectionException(e);
//        }
//
//    }

    @Override
    public boolean execute(String query) throws SQLException
    {
        currentResultSet = executeQuery(query);
        return true;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException
    {

        if (!(autoGeneratedKeys == RETURN_GENERATED_KEYS || autoGeneratedKeys == NO_GENERATED_KEYS))
            throw new SQLSyntaxErrorException(Utils.BAD_AUTO_GEN);

        if (autoGeneratedKeys == RETURN_GENERATED_KEYS) throw new SQLFeatureNotSupportedException(Utils.NO_GEN_KEYS);

        return execute(sql);
    }

    @Override
    public int[] executeBatch() throws SQLException
    {
        throw new SQLFeatureNotSupportedException(Utils.NO_BATCH);
    }

    @Override
    public ResultSet executeQuery(String query) throws SQLException
    {
        com.datastax.driver.core.ResultSet rst = connection.execute(query, consistencyLevel);

        if (null == rst) {
            throw new SQLNonTransientException(Utils.NO_RESULTSET);
        }

        currentResultSet = new CassandraResultSet(new CassandraStatement(connection, query), rst);
        return currentResultSet;

    }

    @Override
    public int executeUpdate(String query) throws SQLException
    {
        checkNotClosed();

        com.datastax.driver.core.ResultSet rst = connection.execute(query, connection.defaultConsistencyLevel);

        if ((null == rst) || !rst.wasApplied()) {
            throw new SQLNonTransientException(Utils.NO_UPDATE_COUNT);
        }

        return rst.all().size();

    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException
    {
        checkNotClosed();

        if (!(autoGeneratedKeys == RETURN_GENERATED_KEYS || autoGeneratedKeys == NO_GENERATED_KEYS))
            throw new SQLFeatureNotSupportedException(Utils.BAD_AUTO_GEN);

        return executeUpdate(sql);
    }

    @Override
    public Connection getConnection() throws SQLException
    {
        checkNotClosed();
        return connection;
    }

    @Override
    public int getFetchDirection() throws SQLException
    {
        checkNotClosed();
        return fetchDirection;
    }

    @Override
    public int getFetchSize() throws SQLException
    {
        checkNotClosed();
        return fetchSize;
    }

    @Override
    public int getMaxFieldSize() throws SQLException
    {
        checkNotClosed();
        return maxFieldSize;
    }

    @Override
    public int getMaxRows() throws SQLException
    {
        checkNotClosed();
        return maxRows;
    }

    @Override
    public boolean getMoreResults() throws SQLException
    {
        checkNotClosed();
        resetResults();
        // in the current Cassandra implementation there are never MORE results
        return false;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException
    {
        checkNotClosed();

        switch (current)
        {
            case CLOSE_CURRENT_RESULT:
                resetResults();
                break;

            case CLOSE_ALL_RESULTS:
            case KEEP_CURRENT_RESULT:
                throw new SQLFeatureNotSupportedException(Utils.NO_MULTIPLE);

            default:
                throw new SQLSyntaxErrorException(String.format(Utils.BAD_KEEP_RSET, current));
        }
        // in the current Cassandra implementation there are never MORE results
        return false;
    }

    @Override
    public int getQueryTimeout() throws SQLException
    {
        // the Cassandra implementation does not support timeouts on queries
        return 0;
    }

    @Override
    public ResultSet getResultSet() throws SQLException
    {
        checkNotClosed();
        return currentResultSet;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException
    {
        checkNotClosed();
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetHoldability() throws SQLException
    {
        checkNotClosed();
        // the Cassandra implementations does not support commits so this is the closest match
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getResultSetType() throws SQLException
    {
        checkNotClosed();
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getUpdateCount() throws SQLException
    {
        checkNotClosed();
        return updateCount;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException
    {
        checkNotClosed();
        return null;
    }

    @Override
    public boolean isClosed()
    {
        return connection == null;
    }

    @Override
    public boolean isPoolable() throws SQLException
    {
        checkNotClosed();
        return false;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        return false;
    }

    protected final void resetResults()
    {
        updateCount = -1;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException
    {
        checkNotClosed();
        // the Cassandra implementation does not currently look at this
        escapeProcessing = enable;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException
    {
        checkNotClosed();

        if (direction == ResultSet.FETCH_FORWARD || direction == ResultSet.FETCH_REVERSE || direction == ResultSet.FETCH_UNKNOWN)
        {
            if ((getResultSetType() == ResultSet.TYPE_FORWARD_ONLY) && (direction != ResultSet.FETCH_FORWARD))
                throw new SQLSyntaxErrorException(String.format(Utils.BAD_FETCH_DIR, direction));
            fetchDirection = direction;
        }
        else throw new SQLSyntaxErrorException(String.format(Utils.BAD_FETCH_DIR, direction));
    }

    @Override
    public void setFetchSize(int size) throws SQLException
    {
        checkNotClosed();
        if (size < 0) throw new SQLSyntaxErrorException(String.format(Utils.BAD_FETCH_SIZE, size));
        fetchSize = size;
    }

    @Override
    public void setMaxFieldSize(int arg0) throws SQLException
    {
        checkNotClosed();
        // silently ignore this setting. always use default 0 (unlimited)
    }

    @Override
    public void setMaxRows(int arg0) throws SQLException
    {
        checkNotClosed();
        // silently ignore this setting. always use default 0 (unlimited)
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException
    {
        checkNotClosed();
        // silently ignore any attempt to set this away from the current default (false)
    }

    @Override
    public void setQueryTimeout(int arg0) throws SQLException
    {
        checkNotClosed();
        // silently ignore any attempt to set this away from the current default (0)
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        if(iface.isInstance(this)) return iface.cast(this);
    	throw new SQLFeatureNotSupportedException(String.format(Utils.NO_INTERFACE, iface.getSimpleName()));
    }

    @Override
    public ConsistencyLevel getConsistencyLevel()
    {
        return consistencyLevel;
    }

    @Override
    public void setConsistencyLevel(ConsistencyLevel consistencyLevel)
    {
        this.consistencyLevel = consistencyLevel;
    }

    @Override
    public int compareTo(Object target)
    {
        if (this.equals(target)) return 0;
        if (this.hashCode()< target.hashCode()) return -1;
        else return 1;
    }
}
