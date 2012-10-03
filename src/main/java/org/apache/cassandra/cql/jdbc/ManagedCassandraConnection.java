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

import static org.apache.cassandra.cql.jdbc.Utils.NO_INTERFACE;
import static org.apache.cassandra.cql.jdbc.Utils.WAS_CLOSED_CON;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

public class ManagedCassandraConnection extends AbstractCassandraConnection implements Connection
{
	private PooledCassandraConnection pooledCassandraConnection;

	private Connection physicalConnection;

	private Set<Statement> statements = new ConcurrentSkipListSet<Statement>();

	public ManagedCassandraConnection(PooledCassandraConnection pooledCassandraConnection)
	{
		this.pooledCassandraConnection = pooledCassandraConnection;
		this.physicalConnection = pooledCassandraConnection.getConnection();
	}

	private void checkClosed() throws SQLNonTransientConnectionException
	{
		if (isClosed())
		{
			throw new SQLNonTransientConnectionException(WAS_CLOSED_CON);
		}
	}

	@Override
	public synchronized void close() throws SQLException
	{
		for (Statement statement : statements)
		{
			if (!statement.isClosed()) {
				statement.close();
			}
		}
		pooledCassandraConnection.connectionClosed();
		pooledCassandraConnection = null;
		physicalConnection = null;
	}

	@Override
	public boolean isClosed()
	{
		return physicalConnection == null;
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException
	{
		throw new SQLFeatureNotSupportedException(String.format(NO_INTERFACE, iface.getSimpleName()));
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException
	{
		return false;
	}

	@Override
	public Statement createStatement() throws SQLException
	{
		checkClosed();
		Statement statement = pooledCassandraConnection.createStatement();
		statements.add(statement);
		return statement;
	}

	@Override
	public PreparedStatement prepareStatement(String cql) throws SQLException
	{
		checkClosed();
		PreparedStatement statement = pooledCassandraConnection.prepareStatement(cql);
		statements.add(statement);
		return statement;
	}

	@Override
	public String nativeSQL(String sql) throws SQLException
	{
		checkClosed();
		try
		{
			return physicalConnection.nativeSQL(sql);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.connectionErrorOccurred(sqlException);
			throw sqlException;
		}
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException
	{
		checkClosed();
		try
		{
			physicalConnection.setAutoCommit(autoCommit);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.connectionErrorOccurred(sqlException);
			throw sqlException;
		}
	}

	@Override
	public boolean getAutoCommit() throws SQLException
	{
		checkClosed();
		try
		{
			return physicalConnection.getAutoCommit();
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.connectionErrorOccurred(sqlException);
			throw sqlException;
		}
	}

	@Override
	public void commit() throws SQLException
	{
		checkClosed();
		try
		{
			physicalConnection.commit();
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.connectionErrorOccurred(sqlException);
			throw sqlException;
		}
	}

	@Override
	public void rollback() throws SQLException
	{
		checkClosed();
		try
		{
			physicalConnection.rollback();
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.connectionErrorOccurred(sqlException);
			throw sqlException;
		}
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException
	{
		checkClosed();
		try
		{
			return physicalConnection.getMetaData();
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.connectionErrorOccurred(sqlException);
			throw sqlException;
		}
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException
	{
		checkClosed();
		try
		{
			physicalConnection.setReadOnly(readOnly);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.connectionErrorOccurred(sqlException);
			throw sqlException;
		}
	}

	@Override
	public boolean isReadOnly() throws SQLException
	{
		checkClosed();
		try
		{
			return physicalConnection.isReadOnly();
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.connectionErrorOccurred(sqlException);
			throw sqlException;
		}
	}

	@Override
	public void setCatalog(String catalog) throws SQLException
	{
		checkClosed();
		try
		{
			physicalConnection.setCatalog(catalog);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.connectionErrorOccurred(sqlException);
			throw sqlException;
		}
	}

	@Override
	public String getCatalog() throws SQLException
	{
		checkClosed();
		try
		{
			return physicalConnection.getCatalog();
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.connectionErrorOccurred(sqlException);
			throw sqlException;
		}
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException
	{
		checkClosed();
		try
		{
			physicalConnection.setTransactionIsolation(level);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.connectionErrorOccurred(sqlException);
			throw sqlException;
		}
	}

	@Override
	public int getTransactionIsolation() throws SQLException
	{
		checkClosed();
		try
		{
			return physicalConnection.getTransactionIsolation();
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.connectionErrorOccurred(sqlException);
			throw sqlException;
		}
	}

	@Override
	public SQLWarning getWarnings() throws SQLException
	{
		checkClosed();
		try
		{
			return physicalConnection.getWarnings();
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.connectionErrorOccurred(sqlException);
			throw sqlException;
		}
	}

	@Override
	public void clearWarnings() throws SQLException
	{
		checkClosed();
		try
		{
			physicalConnection.clearWarnings();
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.connectionErrorOccurred(sqlException);
			throw sqlException;
		}
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException
	{
		checkClosed();
		Statement statement = pooledCassandraConnection.createStatement(resultSetType, resultSetConcurrency);
		statements.add(statement);
		return statement;
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException
	{
		throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
	}

	@Override
	public void setHoldability(int holdability) throws SQLException
	{
		checkClosed();
		try
		{
			physicalConnection.setHoldability(holdability);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.connectionErrorOccurred(sqlException);
			throw sqlException;
		}
	}

	@Override
	public int getHoldability() throws SQLException
	{
		checkClosed();
		try
		{
			return physicalConnection.getHoldability();
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.connectionErrorOccurred(sqlException);
			throw sqlException;
		}
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
	{
		checkClosed();
		Statement statement = pooledCassandraConnection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
		statements.add(statement);
		return statement;
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException
	{
		throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
	}

	@Override
	public boolean isValid(int timeout) throws SQLException
	{
		checkClosed();
		try
		{
			return physicalConnection.isValid(timeout);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.connectionErrorOccurred(sqlException);
			throw sqlException;
		}
	}

	@Override
	public void setClientInfo(String name, String value) throws SQLClientInfoException
	{
		if (!isClosed())
		{
			physicalConnection.setClientInfo(name, value);
		}
	}

	@Override
	public void setClientInfo(Properties properties) throws SQLClientInfoException
	{
		if (!isClosed())
		{
			physicalConnection.setClientInfo(properties);
		}
	}

	@Override
	public String getClientInfo(String name) throws SQLException
	{
		checkClosed();
		try
		{
			return physicalConnection.getClientInfo(name);
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.connectionErrorOccurred(sqlException);
			throw sqlException;
		}
	}

	@Override
	public Properties getClientInfo() throws SQLException
	{
		checkClosed();
		try
		{
			return physicalConnection.getClientInfo();
		}
		catch (SQLException sqlException)
		{
			pooledCassandraConnection.connectionErrorOccurred(sqlException);
			throw sqlException;
		}
	}
}
