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

import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlPreparedResult;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTimeoutException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import static org.apache.cassandra.cql.jdbc.CassandraResultSet.DEFAULT_CONCURRENCY;
import static org.apache.cassandra.cql.jdbc.CassandraResultSet.DEFAULT_HOLDABILITY;
import static org.apache.cassandra.cql.jdbc.CassandraResultSet.DEFAULT_TYPE;
import static org.apache.cassandra.cql.jdbc.Utils.ALWAYS_AUTOCOMMIT;
import static org.apache.cassandra.cql.jdbc.Utils.BAD_TIMEOUT;
import static org.apache.cassandra.cql.jdbc.Utils.NO_INTERFACE;
import static org.apache.cassandra.cql.jdbc.Utils.NO_TRANSACTIONS;
import static org.apache.cassandra.cql.jdbc.Utils.PROTOCOL;
import static org.apache.cassandra.cql.jdbc.Utils.TAG_ACTIVE_CQL_VERSION;
import static org.apache.cassandra.cql.jdbc.Utils.TAG_CONSISTENCY_LEVEL;
import static org.apache.cassandra.cql.jdbc.Utils.TAG_CQL_VERSION;
import static org.apache.cassandra.cql.jdbc.Utils.TAG_DATABASE_NAME;
import static org.apache.cassandra.cql.jdbc.Utils.TAG_PASSWORD;
import static org.apache.cassandra.cql.jdbc.Utils.TAG_PORT_NUMBER;
import static org.apache.cassandra.cql.jdbc.Utils.TAG_SERVER_NAME;
import static org.apache.cassandra.cql.jdbc.Utils.TAG_USER;
import static org.apache.cassandra.cql.jdbc.Utils.WAS_CLOSED_CON;
import static org.apache.cassandra.cql.jdbc.Utils.createSubName;
import static org.apache.cassandra.cql.jdbc.Utils.determineCurrentKeyspace;


/**
 * Implementation class for {@link Connection}.
 */
class CassandraConnection extends AbstractConnection implements Connection
{

    private static final Logger logger = LoggerFactory.getLogger(CassandraConnection.class);

    static final String IS_VALID_CQLQUERY_2_0_0 = "SELECT COUNT(1) FROM system.Versions WHERE component = 'cql';";
    static final String IS_VALID_CQLQUERY_3_0_0 = "SELECT COUNT(1) FROM system.\"Versions\" WHERE component = 'cql';";
    
    public static final int DB_MAJOR_VERSION = 1;
    public static final int DB_MINOR_VERSION = 2;
    public static final String DB_PRODUCT_NAME = "Cassandra";
    public static final String DEFAULT_CQL_VERSION = "3.0.0";

    public static Compression defaultCompression = Compression.GZIP;

    private final boolean autoCommit = true;

    private final int transactionIsolation = Connection.TRANSACTION_NONE;

    /**
     * Connection Properties
     */
    private Properties connectionProps;

    /**
     * Client Info Properties (currently unused)
     */
    private Properties clientInfo = new Properties();

    /**
     * Set of all Statements that have been created by this connection
     */
    private Set<Statement> statements = new ConcurrentSkipListSet<Statement>();

	/**
     * Ohter node info for defined keyspace, null if no keyspace is provided
     */
	private Set<String> otherNodes = new HashSet<String>();

	/**
     * remote connection fields
     */
    private Cassandra.Client client;
    private TTransport transport;
    private TSocket socket;

    protected long timeOfLastFailure = 0;
    protected int numFailures = 0;
    protected String username = null;
    protected String url = null;
    protected String cluster;//current catalog
    protected String currentKeyspace;//current schema
    int majorCqlVersion;
    ColumnDecoder decoder;

    
    PreparedStatement isAlive = null;
    
    private String currentCqlVersion;
    
    ConsistencyLevel defaultConsistencyLevel;

    /**
     * Instantiates a new CassandraConnection.
     */
    public CassandraConnection(Properties props) throws SQLException
    {
        connectionProps = (Properties)props.clone();
        clientInfo = new Properties();
        url = PROTOCOL + createSubName(props);
        try
        {
            String host = props.getProperty(TAG_SERVER_NAME);
            int port = Integer.parseInt(props.getProperty(TAG_PORT_NUMBER));
            currentKeyspace = props.getProperty(TAG_DATABASE_NAME);
            username = props.getProperty(TAG_USER);
            String password = props.getProperty(TAG_PASSWORD);
            String version = props.getProperty(TAG_CQL_VERSION,DEFAULT_CQL_VERSION);
            connectionProps.setProperty(TAG_ACTIVE_CQL_VERSION, version);
            majorCqlVersion = getMajor(version);
            defaultConsistencyLevel = ConsistencyLevel.valueOf(props.getProperty(TAG_CONSISTENCY_LEVEL,ConsistencyLevel.ONE.name()));

            createSocketWIthTransportAndClient(host, port);
            
            cluster = client.describe_cluster_name();

            if (username != null)
            {
                Map<String, String> credentials = new HashMap<String, String>();
                credentials.put("username", username);
                if (password != null) credentials.put("password", password);
                AuthenticationRequest areq = new AuthenticationRequest(credentials);
                client.login(areq);
            }
            
            if (majorCqlVersion > 2)
            {
                client.set_cql_version(version);
            }

            decoder = new ColumnDecoder(client.describe_keyspaces());
                    
            if (currentKeyspace != null) 
            {
            	try
            	{
            		List<TokenRange> ringInfo = client.describe_ring(currentKeyspace);//collect info in case current node fails
            		for (TokenRange tokenRange : ringInfo) 
            		{
						if (tokenRange.isSetRpc_endpoints())
						{
							otherNodes.addAll(tokenRange.getRpc_endpoints());
						}
						if (otherNodes.size() > 5) break; //enough info incase of failure
					}
            		
            		//remove target node if present
                	otherNodes.remove(socket.getSocket().getInetAddress().getHostAddress());
            	}
                catch (InvalidRequestException e)
                {
                	//silently ignore
                }
            }

            Object[] args = {host, port,currentKeyspace,cluster,version, defaultConsistencyLevel.name()};
            logger.debug("Connected to {}:{} in Cluster '{}' using Keyspace '{}', CQL version '{}' and Consistency level {}",args);                       
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
    
    private void createSocketWIthTransportAndClient(String host, int port) throws TException 
    {
        socket = new TSocket(host, port);
        transport = new TFramedTransport(socket);
        TProtocol protocol = new TBinaryProtocol(transport);
        client = new Cassandra.Client(protocol);
        socket.open();
        
        try
        {
	        if (currentKeyspace != null) 
	        {
	        	client.set_keyspace(currentKeyspace);
	        }
		} 
        catch (InvalidRequestException e) 
        {
			//should never happen
		}
	}

	// get the Major portion of a string like : Major.minor.patch where 2 is the default
    private final int getMajor(String version)
    {
        int major = 0;
        String[] parts = version.split("\\.");
        try
        {
            major = Integer.valueOf(parts[0]);
        }
        catch (Exception e)
        {
            major = 2;
        }
        return major;
    }
    
    private final void checkNotClosed() throws SQLException
    {
        if (isClosed()) throw new SQLNonTransientConnectionException(WAS_CLOSED_CON);
    }

    public void clearWarnings() throws SQLException
    {
        // This implementation does not support the collection of warnings so clearing is a no-op
        // but it is still an exception to call this on a closed connection.
        checkNotClosed();
    }

    /**
     * On close of connection.
     */
    public synchronized void close() throws SQLException
    {
        // close all statements associated with this connection upon close
        for (Statement statement : statements)
            statement.close();
        statements.clear();
        
        if (isConnected())
        {
            // then disconnect from the transport                
            disconnect();
        }
    }

    public void commit() throws SQLException
    {
        checkNotClosed();
        throw new SQLFeatureNotSupportedException(ALWAYS_AUTOCOMMIT);
    }

    public Statement createStatement() throws SQLException
    {
        checkNotClosed();
        Statement statement = new CassandraStatement(this);
        statements.add(statement);
        return statement;
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException
    {
        checkNotClosed();
        Statement statement = new CassandraStatement(this, null, resultSetType, resultSetConcurrency);
        statements.add(statement);
        return statement;
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
    {
        checkNotClosed();
        Statement statement = new CassandraStatement(this, null, resultSetType, resultSetConcurrency, resultSetHoldability);
        statements.add(statement);
        return statement;
    }

    public boolean getAutoCommit() throws SQLException
    {
        checkNotClosed();
        return autoCommit;
    }

    public Properties getConnectionProps()
    {
        return connectionProps;
    }

    public String getCatalog() throws SQLException
    {
        checkNotClosed();
        return cluster;
    }

    public void setSchema(String schema) throws SQLException
    {
        checkNotClosed();
        currentKeyspace = schema;
    }

    public String getSchema() throws SQLException
    {
        checkNotClosed();
        return currentKeyspace;
    }

    public Properties getClientInfo() throws SQLException
    {
        checkNotClosed();
        return clientInfo;
    }

    public String getClientInfo(String label) throws SQLException
    {
        checkNotClosed();
        return clientInfo.getProperty(label);
    }

    public int getHoldability() throws SQLException
    {
        checkNotClosed();
        // the rationale is there are really no commits in Cassandra so no boundary...
        return DEFAULT_HOLDABILITY;
    }

    public DatabaseMetaData getMetaData() throws SQLException
    {
        checkNotClosed();
        return new CassandraDatabaseMetaData(this);
    }

    public int getTransactionIsolation() throws SQLException
    {
        checkNotClosed();
        return transactionIsolation;
    }

    public SQLWarning getWarnings() throws SQLException
    {
        checkNotClosed();
        // the rationale is there are no warnings to return in this implementation...
        return null;
    }

    public synchronized boolean isClosed() throws SQLException
    {

        return !isConnected();
    }

    public boolean isReadOnly() throws SQLException
    {
        checkNotClosed();
        return false;
    }

    public boolean isValid(int timeout) throws SQLTimeoutException
    {
        if (timeout < 0) throw new SQLTimeoutException(BAD_TIMEOUT);

        // set timeout
        socket.setTimeout(timeout * 1000);

        try
        {
        	if (isClosed()) {
        		return false;
        	}
        	
            if (isAlive == null)
            {
                isAlive = prepareStatement(currentCqlVersion == "2.0.0" ? IS_VALID_CQLQUERY_2_0_0 : IS_VALID_CQLQUERY_3_0_0);
            }
            // the result is not important
            isAlive.executeQuery().close();
        }
        catch (SQLException e)
        {
        	return false;
        }
        finally {
            // reset timeout
            socket.setTimeout(0);
        }

        return true;
    }

    public boolean isWrapperFor(Class<?> arg0) throws SQLException
    {
        return false;
    }

    public String nativeSQL(String sql) throws SQLException
    {
        checkNotClosed();
        // the rationale is there are no distinction between grammars in this implementation...
        // so we are just return the input argument
        return sql;
    }

    public CassandraPreparedStatement prepareStatement(String cql) throws SQLException
    {
        return prepareStatement(cql,DEFAULT_TYPE,DEFAULT_CONCURRENCY,DEFAULT_HOLDABILITY);
    }

    public CassandraPreparedStatement prepareStatement(String cql, int rsType) throws SQLException
    {
        return prepareStatement(cql,rsType,DEFAULT_CONCURRENCY,DEFAULT_HOLDABILITY);
    }

    public CassandraPreparedStatement prepareStatement(String cql, int rsType, int rsConcurrency) throws SQLException
    {
        return prepareStatement(cql,rsType,rsConcurrency,DEFAULT_HOLDABILITY);
    }

    public CassandraPreparedStatement prepareStatement(String cql, int rsType, int rsConcurrency, int rsHoldability) throws SQLException
    {
        checkNotClosed();
        CassandraPreparedStatement statement = new CassandraPreparedStatement(this, cql, rsType,rsConcurrency,rsHoldability);
        statements.add(statement);
        return statement;
    }

    public void rollback() throws SQLException
    {
        checkNotClosed();
        throw new SQLFeatureNotSupportedException(ALWAYS_AUTOCOMMIT);
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException
    {
        checkNotClosed();
        if (!autoCommit) throw new SQLFeatureNotSupportedException(ALWAYS_AUTOCOMMIT);
    }

    public void setCatalog(String arg0) throws SQLException
    {
        checkNotClosed();
        // the rationale is there are no catalog name to set in this implementation...
        // so we are "silently ignoring" the request
    }

    public void setClientInfo(Properties props) throws SQLClientInfoException
    {
        // we don't use them but we will happily collect them for now...
        if (props != null) clientInfo = props;
    }

    public void setClientInfo(String key, String value) throws SQLClientInfoException
    {
        // we don't use them but we will happily collect them for now...
        clientInfo.setProperty(key, value);
    }

    public void setHoldability(int arg0) throws SQLException
    {
        checkNotClosed();
        // the rationale is there are no holdability to set in this implementation...
        // so we are "silently ignoring" the request
    }

    public void setReadOnly(boolean arg0) throws SQLException
    {
        checkNotClosed();
        // the rationale is all connections are read/write in the Cassandra implementation...
        // so we are "silently ignoring" the request
    }

    public void setTransactionIsolation(int level) throws SQLException
    {
        checkNotClosed();
        if (level != Connection.TRANSACTION_NONE) throw new SQLFeatureNotSupportedException(NO_TRANSACTIONS);
    }

    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(String.format(NO_INTERFACE, iface.getSimpleName()));
    }

    /**
     * Execute a CQL query.
     *
     * @param queryStr    a CQL query string
     * @param consistencyLevel	the CQL query consistency level
     * @param compression query compression to use
     * @return the query results encoded as a CqlResult structure
     * @throws InvalidRequestException     on poorly constructed or illegal requests
     * @throws UnavailableException        when not all required replicas could be created/read
     * @throws TimedOutException           when a cluster operation timed out
     * @throws SchemaDisagreementException when the client side and server side are at different versions of schema (Thrift)
     * @throws TException                  when there is a error in Thrift processing
     */
    private CqlResult execute(String queryStr, Compression compression, ConsistencyLevel consistencyLevel, int attempt) throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        currentKeyspace = determineCurrentKeyspace(queryStr, currentKeyspace);

        try
        {
            if (majorCqlVersion==3) return client.execute_cql3_query(Utils.compressQuery(queryStr, compression), compression, consistencyLevel);
            else                    return client.execute_cql_query(Utils.compressQuery(queryStr, compression), compression);
        }
        catch (TException e)
        {
            numFailures++;
            timeOfLastFailure = System.currentTimeMillis();
            if (e instanceof TTransportException && attempt < 3 && knowsMoreNodes())
            {
            	attempt++;
            	connectToOtherNode();
            	return execute(queryStr, compression, consistencyLevel, attempt);
            }
            else
            {
            	throw e;
            }
        }
    }

    /**
     * Execute a CQL query using the default compression methodology.
     *
     * @param queryStr a CQL query string
     * @param consistencyLevel	the CQL query consistency level
     * @return the query results encoded as a CqlResult structure
     * @throws InvalidRequestException     on poorly constructed or illegal requests
     * @throws UnavailableException        when not all required replicas could be created/read
     * @throws TimedOutException           when a cluster operation timed out
     * @throws SchemaDisagreementException when the client side and server side are at different versions of schema (Thrift)
     * @throws TException                  when there is a error in Thrift processing
     */
    protected CqlResult execute(String queryStr, ConsistencyLevel consistencyLevel)
    		throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        return execute(queryStr, defaultCompression, consistencyLevel,0);
    }

    protected CqlResult execute(CassandraPreparedStatement cps, List<ByteBuffer> values, ConsistencyLevel consistencyLevel)
    		throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
    	return execute(cps, values, consistencyLevel, 0);
    }
    private CqlResult execute(CassandraPreparedStatement cps, List<ByteBuffer> values, ConsistencyLevel consistencyLevel, int attempt)
    		throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        try
        {
        	int itemId = cps.getItemId();
            if (majorCqlVersion==3) return client.execute_prepared_cql3_query(itemId, values, consistencyLevel);
            else                    return client.execute_prepared_cql_query(itemId, values);
        }
        catch (TException e)
        {
            numFailures++;
            timeOfLastFailure = System.currentTimeMillis();
            if (e instanceof TTransportException && attempt < 3 && knowsMoreNodes())
            {
            	attempt++;
            	connectToOtherNode();
            	cps.createPreparedResult();//fixup internals
            	return execute(cps, values, consistencyLevel, attempt);
            }
            else
            {
            	throw e;
            }
        }
    }

	private boolean knowsMoreNodes()
	{
		if (otherNodes.size() == 0) 
		{
			return false;
		}
		return true;
	}

    private void connectToOtherNode() throws TException 
    {
    	Iterator<String> it = otherNodes.iterator();
    	if (it.hasNext())
    	{
	    	String newHost = it.next();
	    	it.remove(); //make sure we do not retry incase this one also fails
	    	
	    	int port = Integer.parseInt(connectionProps.getProperty(TAG_PORT_NUMBER));
			createSocketWIthTransportAndClient(newHost, port);
    	}
	}

	protected CqlPreparedResult prepare(String queryStr, Compression compression) throws InvalidRequestException, TException
	{
		return prepare(queryStr, compression, 0);
	}
	private CqlPreparedResult prepare(String queryStr, Compression compression, int attempt) throws InvalidRequestException, TException
    {
        try
        {
            if (majorCqlVersion==3) return client.prepare_cql3_query(Utils.compressQuery(queryStr, compression), compression);
            else                    return client.prepare_cql_query(Utils.compressQuery(queryStr, compression), compression);
        }
        catch (TException e)
        {
            numFailures++;
            timeOfLastFailure = System.currentTimeMillis();
            if (e instanceof TTransportException && attempt < 3 && knowsMoreNodes())
            {
            	attempt++;
            	connectToOtherNode();
            	return prepare(queryStr, compression, attempt);
            }
            else
            {
            	throw e;
            }
        }
    }
    
    protected CqlPreparedResult prepare(String queryStr) throws InvalidRequestException, TException
    {
        try
        {
            return prepare(queryStr, defaultCompression);
        }
        catch (TException error)
        {
            numFailures++;
            timeOfLastFailure = System.currentTimeMillis();
            throw error;
        }
    }
    
    /**
     * Remove a Statement from the Open Statements List
     */
    protected boolean removeStatement(Statement statement)
    {
        return statements.remove(statement);
    }
    
    /**
     * Shutdown the remote connection
     */
    protected void disconnect()
    {
        transport.close();
    }

    /**
     * Connection state.
     */
    protected boolean isConnected()
    {
        return transport.isOpen();
    }

    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("CassandraConnection [connectionProps=");
        builder.append(connectionProps);
        builder.append("]");
        return builder.toString();
    }    
}
