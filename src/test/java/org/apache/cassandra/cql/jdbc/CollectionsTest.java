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

package org.apache.cassandra.cql.jdbc;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

import org.apache.cassandra.cql.ConnectionDetails;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test CQL Collections Data Types
 * List
 * Map
 * Set
 * 
 */
public class CollectionsTest
{
    private static final Logger LOG = LoggerFactory.getLogger(CollectionsTest.class);


    private static final String HOST = System.getProperty("host", ConnectionDetails.getHost());
    private static final int PORT = Integer.parseInt(System.getProperty("port", ConnectionDetails.getPort() + ""));
    private static final String KEYSPACE = "testks";
    private static final String SYSTEM = "system";
    private static final String CQLV3 = "3.0.0";

    private static java.sql.Connection con = null;

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
        String URL = String.format("jdbc:cassandra://%s:%d/%s?version=%s", HOST, PORT, SYSTEM, CQLV3);

        con = DriverManager.getConnection(URL);

        LOG.debug("URL         = '{}'", URL);

        Statement stmt = con.createStatement();

        // Use Keyspace
        String useKS = String.format("USE %s;", KEYSPACE);

        // Drop Keyspace
        String dropKS = String.format("DROP KEYSPACE %s;", KEYSPACE);

        try
        {
            stmt.execute(dropKS);
        }
        catch (Exception e)
        {/* Exception on DROP is OK */}

        // Create KeySpace
        String createKS = String.format("CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy',  'replication_factor' : 1 }",KEYSPACE);
//        String createKS = String.format("CREATE KEYSPACE %s WITH strategy_class = SimpleStrategy AND strategy_options:replication_factor = 1;",KEYSPACE);
        LOG.debug("createKS    = '{}'", createKS);

        stmt = con.createStatement();
        stmt.execute("USE " + SYSTEM);
        stmt.execute(createKS);
        stmt.execute(useKS);


        // Create the target Table (CF)
        String createTable = "CREATE TABLE testcollection (" + " k int PRIMARY KEY," + " L list<bigint>," + " M map<double, boolean>," + " S set<text>" + ") ;";
        LOG.debug("createTable = '{}'", createTable);

        stmt.execute(createTable);
        stmt.close();
        con.close();

        // open it up again to see the new TABLE
        URL = String.format("jdbc:cassandra://%s:%d/%s?version=%s", HOST, PORT, KEYSPACE, CQLV3);
        con = DriverManager.getConnection(URL);
        LOG.debug("URL         = '{}'", URL);

        Statement statement = con.createStatement();

        String insert = "INSERT INTO testcollection (k,L) VALUES( 1,[1, 3, 12345]);";
        statement.executeUpdate(insert);
        String update1 = "UPDATE testcollection SET S = {'red', 'white', 'blue'} WHERE k = 1;";
        String update2 = "UPDATE testcollection SET M = {2.0: 'true', 4.0: 'false', 6.0 : 'true'} WHERE k = 1;";
        statement.executeUpdate(update1);
        statement.executeUpdate(update2);


        LOG.debug("Unit Test: 'CollectionsTest' initialization complete.\n\n");
    }

    /**
     * Close down the connection when complete
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        if (con != null) con.close();
    }

    @Test
    public void testReadList() throws Exception
    {
        if (LOG.isDebugEnabled()) LOG.debug("Test: 'testReadList'.\n");

        Statement statement = con.createStatement();

        ResultSet result = statement.executeQuery("SELECT * FROM testcollection WHERE k = 1;");
        result.next();

        assertEquals(1, result.getInt("k"));

        Object myObj = result.getObject("l");
        LOG.debug("l           = '{}'", myObj);
        List<Long> myList = (List<Long>) myObj;
        assertEquals(3, myList.size());
        assertTrue(12345L == myList.get(2));
        assertTrue(myObj instanceof ArrayList);

        myList = (List<Long>) extras(result).getList("l");
        statement.close();
        assertTrue(3L == myList.get(1));

        if (LOG.isDebugEnabled()) LOG.debug("\n");
    }

    @Test
    public void testUpdateList() throws Exception
    {
        if (LOG.isDebugEnabled()) LOG.debug("Test: 'testUpdateList'.\n");
        
        Statement statement = con.createStatement();

        String update1 = "UPDATE testcollection SET L = L + [2,4,6] WHERE k = 1;";
        statement.executeUpdate(update1);

        ResultSet result = statement.executeQuery("SELECT * FROM testcollection WHERE k = 1;");
        result.next();

        assertEquals(1, result.getInt("k"));
        Object myObj = result.getObject("l");
        List<Long> myList = (List<Long>) myObj;
        assertEquals(6, myList.size());
        assertTrue(12345L == myList.get(2));
        
        LOG.debug("l           = '{}'", myObj);

        String update2 = "UPDATE testcollection SET L = [98,99,100] + L WHERE k = 1;";
        statement.executeUpdate(update2);
        result = statement.executeQuery("SELECT * FROM testcollection WHERE k = 1;");
        result.next();
        myObj = result.getObject("l");
        myList = (List<Long>) myObj;
        assertTrue(100L == myList.get(0));
        
        LOG.debug("l           = '{}'", myObj);

        String update3 = "UPDATE testcollection SET L[0] = 2000 WHERE k = 1;";
        statement.executeUpdate(update3);
        result = statement.executeQuery("SELECT * FROM testcollection WHERE k = 1;");
        result.next();
        myObj = result.getObject("l");
        myList = (List<Long>) myObj;
        
        LOG.debug("l           = '{}'", myObj);
        
//        String update4 = "UPDATE testcollection SET L = L +  ? WHERE k = 1;";
//        
//        PreparedStatement prepared = con.prepareStatement(update4);
//        prepared.setLong(1, 8888L);
//        prepared.executeUpdate();
//
//        result = prepared.executeQuery("SELECT * FROM testcollection WHERE k = 1;");
//        result.next();
//        myObj = result.getObject("l");
//        myList = (List<Long>) myObj;
//        
//        LOG.debug("l           = '{}'", myObj);

        if (LOG.isDebugEnabled()) LOG.debug("\n");
    }

    @Test
    public void testReadSet() throws Exception
    {
        if (LOG.isDebugEnabled()) LOG.debug("Test: 'testReadSet'.\n");

        Statement statement = con.createStatement();


        ResultSet result = statement.executeQuery("SELECT * FROM testcollection WHERE k = 1;");
        result.next();

        assertEquals(1, result.getInt("k"));

        Object myObj = result.getObject("s");
        LOG.debug("s           = '{}'", myObj);
        Set<String> mySet = (Set<String>) myObj;
        assertEquals(3, mySet.size());
        assertTrue(mySet.contains("white"));
        assertTrue(myObj instanceof LinkedHashSet);

        if (LOG.isDebugEnabled()) LOG.debug("\n");
    }

    @Test
    public void testUpdateSet() throws Exception
    {
        if (LOG.isDebugEnabled()) LOG.debug("Test: 'testUpdateSet'.\n");
        
        Statement statement = con.createStatement();

        // add some items to the set
        String update1 = "UPDATE testcollection SET S = S + {'green', 'white', 'orange'} WHERE k = 1;";
        statement.executeUpdate(update1);

        ResultSet result = statement.executeQuery("SELECT * FROM testcollection WHERE k = 1;");
        result.next();

        assertEquals(1, result.getInt("k"));
        Object myObj = result.getObject("s");
        Set<String> mySet = (Set<String>) myObj;
        assertEquals(5, mySet.size());
        assertTrue(mySet.contains("white"));

        LOG.debug("l           = '{}'", myObj);

        // remove an item from the set
        String update2 = "UPDATE testcollection SET S = S - {'red'} WHERE k = 1;";
        statement.executeUpdate(update2);

        result = statement.executeQuery("SELECT * FROM testcollection WHERE k = 1;");
        result.next();

        assertEquals(1, result.getInt("k"));

        myObj = result.getObject("s");
        mySet = (Set<String>) myObj;
        assertEquals(4, mySet.size());
        assertTrue(mySet.contains("white"));
        assertFalse(mySet.contains("red"));

        LOG.debug("s           = '{}'", myObj);

        if (LOG.isDebugEnabled()) LOG.debug("\n");
    }


    private CassandraResultSetExtras extras(ResultSet result) throws Exception
    {
        Class crse = Class.forName("org.apache.cassandra.cql.jdbc.CassandraResultSetExtras");
        return (CassandraResultSetExtras) result.unwrap(crse);
    }

}
