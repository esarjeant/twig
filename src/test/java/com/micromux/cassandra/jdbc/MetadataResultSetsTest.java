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

import com.micromux.cassandra.ConnectionDetails;
import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URLEncoder;
import java.nio.charset.CharacterCodingException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;


public class MetadataResultSetsTest
{
    private static final String HOST = System.getProperty("host", ConnectionDetails.getHost());
    private static final int PORT = Integer.parseInt(System.getProperty("port", ConnectionDetails.getPort()+""));
    private static final String KEYSPACE1 = "testks1";
    private static final String KEYSPACE2 = "testks2";
    private static final String DROP_KS = "DROP KEYSPACE \"%s\";";
    private static final String CREATE_KS = "CREATE KEYSPACE \"%s\" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};";
      
    private static java.sql.Connection con = null;

    // use these for encrypted connections
    private static final String TRUST_STORE = System.getProperty("trustStore");
    private static final String TRUST_PASS = System.getProperty("trustPass", "cassandra");

    private static String OPTIONS = "";


    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {

        // configure OPTIONS
        if (!StringUtils.isEmpty(TRUST_STORE)) {
            OPTIONS = String.format("trustStore=%s&trustPass=%s",
                    URLEncoder.encode(TRUST_STORE), TRUST_PASS);
        }

        Class.forName("com.micromux.cassandra.jdbc.CassandraDriver");
        String URL = String.format("jdbc:cassandra://%s:%d/%s?version=3.0.0&%s",HOST,PORT,"system",OPTIONS);
        System.out.println("Connection URL = '"+URL +"'");
        
        con = DriverManager.getConnection(URL);
        Statement stmt = con.createStatement();
        
        // Drop Keyspace
        String dropKS1 = String.format(DROP_KS,KEYSPACE1);
        String dropKS2 = String.format(DROP_KS,KEYSPACE2);
        
        try { stmt.execute(dropKS1); stmt.execute(dropKS2);}
        catch (Exception e){/* Exception on DROP is OK */}

        // Create KeySpace
        String createKS1 = String.format(CREATE_KS,KEYSPACE1);
        String createKS2 = String.format(CREATE_KS,KEYSPACE2);
        stmt = con.createStatement();
        stmt.execute("USE system;");
        stmt.execute(createKS1);
        stmt.execute(createKS2);
        
        // Use Keyspace
        String useKS1 = String.format("USE \"%s\";",KEYSPACE1);
        String useKS2 = String.format("USE \"%s\";",KEYSPACE2);
        stmt.execute(useKS1);
        
        // Create the target Column family
        String createCF1 = "CREATE COLUMNFAMILY test1 (keyname text PRIMARY KEY," 
                        + " t1bValue boolean,"
                        + " t1iValue int"
                        + ") WITH comment = 'first TABLE in the Keyspace'"
                        + ";";
        
        String createCF2 = "CREATE COLUMNFAMILY test2 (keyname text PRIMARY KEY," 
                        + " t2bValue boolean,"
                        + " t2iValue int"
                        + ") WITH comment = 'second TABLE in the Keyspace'"
                        + ";";
        
        stmt.execute(createCF1);
        stmt.execute(createCF2);
        stmt.execute(useKS2);
        stmt.execute(createCF1);
        stmt.execute(createCF2);

        stmt.close();
        con.close();

        // open it up again to see the new CF
        con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s?version=3.0.0&%s",HOST,PORT,KEYSPACE1,OPTIONS));

    }
    
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        if (con!=null) con.close();
    }

    private final String showColumn(int index, ResultSet result) throws SQLException
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(index).append("]");
        sb.append(result.getObject(index));
        return sb.toString();
    }

    private final String toString(ResultSet result) throws SQLException
    {
       StringBuilder sb = new StringBuilder();

       while (result.next())
       {
           ResultSetMetaData metadata = result.getMetaData();
           int colCount = metadata.getColumnCount();
           sb.append(String.format("(%d) ",result.getRow()));
           for (int i = 1; i <= colCount; i++)
           {
               sb.append(" " +showColumn(i,result)); 
           }
           sb.append("\n");
       }
       return sb.toString();
    }

	private final String getColumnNames(ResultSetMetaData metaData) throws SQLException
	{
       StringBuilder sb = new StringBuilder();
        int count = metaData.getColumnCount();
        for (int i = 1; i <= count; i++) {
            sb.append(metaData.getColumnName(i));
            if (i < count) sb.append(", ");
		}
        return sb.toString();
	}

    // TESTS ------------------------------------------------------------------
    
    @Test
    public void testTableType() throws SQLException
    {
        CassandraStatement statement = (CassandraStatement) con.createStatement();
        ResultSet result = MetadataResultSets.makeTableTypes(statement);
        
        System.out.println("--- testTableType() ---");
        System.out.println(toString(result));       
        System.out.println();
    }

    @Test
    public void testCatalogs() throws SQLException
    {
        CassandraStatement statement = (CassandraStatement) con.createStatement();
        ResultSet result = MetadataResultSets.makeCatalogs(statement);
        
        System.out.println("--- testCatalogs() ---");
        System.out.println(toString(result));       
        System.out.println();
    }

    @Test
    public void testSchemas() throws SQLException
    {
        CassandraStatement statement = (CassandraStatement) con.createStatement();
        ResultSet result = MetadataResultSets.makeSchemas(statement, null);
        
        System.out.println("--- testSchemas() ---");
        System.out.println(getColumnNames(result.getMetaData()));
       
        System.out.println(toString(result));       
        System.out.println();
        
        result = MetadataResultSets.makeSchemas(statement, KEYSPACE2);
        System.out.println(toString(result));       
        System.out.println();
    }

    @Test
    public void testTableName() throws SQLException
    {
        CassandraPreparedStatement statement = (CassandraPreparedStatement) con.prepareStatement("select * from " + KEYSPACE1 + ".test1");
        ResultSet result = statement.executeQuery();

        System.out.println("--- testTableName() ---");
        ResultSetMetaData meta = result.getMetaData();
        assertEquals("test1", meta.getTableName(1));

    }

    @Test
    public void testTables() throws SQLException
    {
        CassandraStatement statement = (CassandraStatement) con.createStatement();
        ResultSet result = MetadataResultSets.makeTables(statement, null, null);
        
        System.out.println("--- testTables() ---");
        System.out.println(getColumnNames(result.getMetaData()));
       
        System.out.println(toString(result));       
        System.out.println();
        
        result = MetadataResultSets.makeTables(statement, KEYSPACE2, null);
        System.out.println(toString(result));       
        System.out.println();

        result = MetadataResultSets.makeTables(statement, null, "test1");
        System.out.println(toString(result));       
        System.out.println();

        result = MetadataResultSets.makeTables(statement, KEYSPACE2, "test1");
        System.out.println(toString(result));       
        System.out.println();
    }

    @Test
    public void testColumns() throws SQLException, CharacterCodingException {

        CassandraStatement statement = (CassandraStatement) con.createStatement();
        ResultSet result = MetadataResultSets.makeColumns(statement, KEYSPACE1, "test1" ,null);
        
        System.out.println("--- testColumns() ---");
        System.out.println(getColumnNames(result.getMetaData()));
       
        System.out.println(toString(result));       
        System.out.println();

        result = MetadataResultSets.makeColumns(statement, KEYSPACE1, "test2" ,null);
        
        System.out.println("--- testColumns() ---");
        System.out.println(getColumnNames(result.getMetaData()));
       
        System.out.println(toString(result));       
        System.out.println();
    }

    @Test
    public void testClob() throws SQLException, CharacterCodingException {

        CassandraStatement statement = (CassandraStatement) con.createStatement();
        ResultSet result = MetadataResultSets.makeColumns(statement, KEYSPACE1, "test3" ,null);

        System.out.println("--- testColumns() ---");
        System.out.println(getColumnNames(result.getMetaData()));

        System.out.println(toString(result));
        System.out.println();

        result = MetadataResultSets.makeColumns(statement, KEYSPACE1, "test3" ,null);

        System.out.println("--- testColumns() ---");
        System.out.println(getColumnNames(result.getMetaData()));

        System.out.println(toString(result));
        System.out.println();
    }
}
