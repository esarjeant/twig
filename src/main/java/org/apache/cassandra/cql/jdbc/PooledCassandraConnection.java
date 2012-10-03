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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import javax.sql.StatementEvent;
import javax.sql.StatementEventListener;

class PooledCassandraConnection implements PooledConnection
{
	private Connection physicalConnection;
	
	Set<ConnectionEventListener> connectionEventListeners = new HashSet<ConnectionEventListener>();
	
	Set<StatementEventListener> statementEventListeners = new HashSet<StatementEventListener>();

	public PooledCassandraConnection(Connection physicalConnection)
	{
		this.physicalConnection = physicalConnection;
	}

	@Override
	public Connection getConnection()
	{
		return physicalConnection;
	}

	@Override
	public void close() throws SQLException
	{
		physicalConnection.close();
	}

	@Override
	public void addConnectionEventListener(ConnectionEventListener listener)
	{
		connectionEventListeners.add(listener);
	}

	@Override
	public void removeConnectionEventListener(ConnectionEventListener listener)
	{
		connectionEventListeners.remove(listener);
	}

	@Override
	public void addStatementEventListener(StatementEventListener listener)
	{
		statementEventListeners.add(listener);
	}

	@Override
	public void removeStatementEventListener(StatementEventListener listener)
	{
		statementEventListeners.remove(listener);
	}

	void connectionClosed ()
	{
		ConnectionEvent event = new ConnectionEvent(this);
		for (ConnectionEventListener listener : connectionEventListeners) {
			listener.connectionClosed(event);
		}
	}
	
	void connectionErrorOccurred (SQLException sqlException)
	{
		ConnectionEvent event = new ConnectionEvent(this, sqlException);
		for (ConnectionEventListener listener : connectionEventListeners) {
			listener.connectionErrorOccurred(event);
		}
	}
	
	void statementClosed (PreparedStatement preparedStatement)
	{
		StatementEvent event = new StatementEvent(this, preparedStatement);
		for (StatementEventListener listener : statementEventListeners) {
			listener.statementClosed(event);
		}
	}
	
	void statementErrorOccurred (PreparedStatement preparedStatement, SQLException sqlException)
	{
		StatementEvent event = new StatementEvent(this, preparedStatement, sqlException);
		for (StatementEventListener listener : statementEventListeners) {
			listener.statementErrorOccurred(event);
		}
	}

	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
	{
		return physicalConnection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException
	{
		return physicalConnection.createStatement(resultSetType, resultSetConcurrency);
	}

	public Statement createStatement() throws SQLException
	{
		return physicalConnection.createStatement();
	}

	public PreparedStatement prepareStatement(String cql) throws SQLException
	{
		return physicalConnection.prepareStatement(cql);
	}
	

}
