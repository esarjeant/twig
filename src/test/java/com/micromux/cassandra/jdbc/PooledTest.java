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

import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PooledTest extends BaseDriverTest
{

	@Before
	public void setUpBeforeTest() throws Exception
	{

		Statement stmt = con.createStatement();

		// Drop Keyspace
		String dropKS = String.format("DROP KEYSPACE \"%s\";", KEYSPACE);

		try
		{
			stmt.execute(dropKS);
		}
		catch (Exception e)
		{/* Exception on DROP is OK */
		}

		// Create KeySpace
		String createKS = String.format(
				"CREATE KEYSPACE \"%s\" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};",
				KEYSPACE);
		stmt.execute(createKS);

		// Create KeySpace
		String useKS = String.format("USE \"%s\";", KEYSPACE);
		stmt.execute(useKS);

		// Create the target Column family
		String createCF = "CREATE COLUMNFAMILY pooled_test (somekey text PRIMARY KEY," + "someInt int"
				+ ") ;";
		stmt.execute(createCF);

		String insertWorld = "UPDATE pooled_test SET someInt = 1 WHERE somekey = 'world'";
		stmt.execute(insertWorld);
	}

	@Test
	public void twoMillionConnections() throws Exception
	{
		CassandraDataSource connectionPoolDataSource = new CassandraDataSource(HOST, PORT, KEYSPACE, USER, PASSWORD, VERSION,CONSISTENCY,TRUST_STORE,TRUST_PASS);

		DataSource pooledCassandraDataSource = new PooledCassandraDataSource(connectionPoolDataSource);

		for (int i = 0; i < 2000000; i++)
		{
			Connection connection = pooledCassandraDataSource.getConnection();
			connection.close();
		}
	}

	@Test
	public void twoMillionPreparedStatements() throws Exception
	{
		CassandraDataSource connectionPoolDataSource = new CassandraDataSource(HOST, PORT, KEYSPACE, USER, PASSWORD, VERSION,CONSISTENCY,TRUST_STORE,TRUST_PASS);

		DataSource pooledCassandraDataSource = new PooledCassandraDataSource(connectionPoolDataSource);

		Connection connection = pooledCassandraDataSource.getConnection();
		for (int i = 0; i < 10000; i++)
		{
			PreparedStatement preparedStatement = connection.prepareStatement("SELECT someInt FROM pooled_test WHERE somekey = ?");
			preparedStatement.close();
		}
		connection.close();
	}

	@Test
	public void preparedStatement() throws Exception
	{
		CassandraDataSource connectionPoolDataSource = new CassandraDataSource(HOST, PORT, KEYSPACE, USER, PASSWORD, VERSION,CONSISTENCY,TRUST_STORE,TRUST_PASS);

		DataSource pooledCassandraDataSource = new PooledCassandraDataSource(connectionPoolDataSource);

		Connection connection = pooledCassandraDataSource.getConnection();

		PreparedStatement statement = connection.prepareStatement("SELECT someint FROM pooled_test WHERE somekey = ?");
		statement.setString(1, "world");

		ResultSet resultSet = statement.executeQuery();
		assertTrue(resultSet.next());
		assertEquals(1, resultSet.getInt(1));
		assertFalse(resultSet.next());
		resultSet.close();

		statement.close();
		connection.close();
	}

	@Test
	public void preparedStatementClose() throws Exception
	{
		CassandraDataSource connectionPoolDataSource = new CassandraDataSource(HOST, PORT, KEYSPACE, USER, PASSWORD, VERSION,CONSISTENCY,TRUST_STORE,TRUST_PASS);

		DataSource pooledCassandraDataSource = new PooledCassandraDataSource(connectionPoolDataSource);

		Connection connection = pooledCassandraDataSource.getConnection();

		PreparedStatement statement = connection.prepareStatement("SELECT someInt FROM pooled_test WHERE somekey = ?");
		statement.setString(1, "world");

		ResultSet resultSet = statement.executeQuery();
		assertTrue(resultSet.next());
		assertEquals(1, resultSet.getInt(1));
		assertFalse(resultSet.next());
		resultSet.close();

		connection.close();
		
		assert statement.isClosed();
	}

	@Test
	public void statement() throws Exception
	{
		CassandraDataSource connectionPoolDataSource = new CassandraDataSource(HOST, PORT, KEYSPACE, USER, PASSWORD, VERSION,CONSISTENCY,TRUST_STORE,TRUST_PASS);

		DataSource pooledCassandraDataSource = new PooledCassandraDataSource(connectionPoolDataSource);

		Connection connection = pooledCassandraDataSource.getConnection();

		Statement statement = connection.createStatement();

		ResultSet resultSet = statement.executeQuery("SELECT someInt FROM pooled_test WHERE somekey = 'world'");
		assertTrue(resultSet.next());
		assertEquals(1, resultSet.getInt(1));
		assertFalse(resultSet.next());
		resultSet.close();

		statement.close();
		connection.close();
	}

	@Test
	public void statementClosed() throws Exception
	{
		CassandraDataSource connectionPoolDataSource = new CassandraDataSource(HOST, PORT, KEYSPACE, USER, PASSWORD, VERSION,CONSISTENCY,TRUST_STORE,TRUST_PASS);

		DataSource pooledCassandraDataSource = new PooledCassandraDataSource(connectionPoolDataSource);

		Connection connection = pooledCassandraDataSource.getConnection();

		Statement statement = connection.createStatement();

		ResultSet resultSet = statement.executeQuery("SELECT someInt FROM pooled_test WHERE somekey = 'world'");
		assertTrue(resultSet.next());
		assertEquals(1, resultSet.getInt(1));
		assertFalse(resultSet.next());
		resultSet.close();

		connection.close();
		
		assert statement.isClosed();
	}
}
