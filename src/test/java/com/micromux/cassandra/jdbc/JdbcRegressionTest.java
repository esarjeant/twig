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
import com.micromux.cassandra.ConnectionDetails;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URLEncoder;
import java.sql.*;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class JdbcRegressionTest
{
    private static final String HOST = System.getProperty("host", ConnectionDetails.getHost());
    private static final int PORT = Integer.parseInt(System.getProperty("port", ConnectionDetails.getPort()+""));
    private static final String KEYSPACE = "testks";
    private static final String TABLE = "regressiontest";
    private static final String TYPETABLE = "datatypetest";

    // use these for encrypted connections
    private static final String TRUST_STORE = System.getProperty("trustStore");
    private static final String TRUST_PASS = System.getProperty("trustPass", "cassandra");

    private static String OPTIONS = "";

    private static final String CONSISTENCY_QUORUM = "QUORUM";
      
    private static java.sql.Connection con = null;
    

    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {

        // configure OPTIONS
        if (!StringUtils.isEmpty(TRUST_STORE)) {
            OPTIONS = String.format("trustStore=%s&trustPass=%s",
                    URLEncoder.encode(TRUST_STORE), TRUST_PASS);
        }

        Class.forName("com.micromux.cassandra.jdbc.CassandraDriver");
        String URL = String.format("jdbc:cassandra://%s:%d/%s?%s", HOST, PORT, "system", OPTIONS);
        System.out.println("Connection URL = '"+URL +"'");
        
        con = DriverManager.getConnection(URL);
        Statement stmt = con.createStatement();
        
        // Drop Keyspace
        String dropKS = String.format("DROP KEYSPACE \"%s\";",KEYSPACE);
        
        try { stmt.execute(dropKS);}
        catch (Exception e){/* Exception on DROP is OK */}

        // Create KeySpace
        String createKS = String.format("CREATE KEYSPACE \"%s\" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};",KEYSPACE);
        System.out.println("createKS = '"+createKS+"'");
        stmt = con.createStatement();
        stmt.execute("USE system;");
        stmt.execute(createKS);
        
        // Use Keyspace
        String useKS = String.format("USE \"%s\";",KEYSPACE);
        stmt.execute(useKS);
        
        // Create the target Column family
        String createCF = "CREATE COLUMNFAMILY "+TABLE+" (keyname text PRIMARY KEY,"
                        + " bValue boolean,"
                        + " iValue int"
                        + ");";
        stmt.execute(createCF);

        //create an index
        stmt.execute("CREATE INDEX ON "+TABLE+" (iValue)");

        String createCF2 = "CREATE COLUMNFAMILY " + TYPETABLE + " ( "
                + " id uuid PRIMARY KEY, "
                + " blobValue blob,"
                + " blobSetValue set<blob>,"
                + " dataMapValue map<text,blob>,"
                + ") WITH comment = 'datatype TABLE in the Keyspace'"
                + ";";
        stmt.execute(createCF2);

        stmt.close();
        con.close();

        // open it up again to see the new CF
        con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s?%s", HOST, PORT, KEYSPACE, OPTIONS));
        System.out.println(con);

    }
    
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        if (con!=null) con.close();
    }

    /**
     * Test an update statement; confirm that the results can be read back.
     */
    @Test
    public void testExecuteUpdate() throws Exception
    {
        String insert = "INSERT INTO regressiontest (keyname,bValue,iValue) VALUES( 'key0',true, 2000);";
        Statement statement = con.createStatement();

        statement.executeUpdate(insert);
        statement.close();
        
        statement = con.createStatement();
        ResultSet result = statement.executeQuery("SELECT bValue,iValue FROM regressiontest WHERE keyname='key0';");
        result.next();
        
        boolean b = result.getBoolean(1);
        assertTrue(b);
        
        int i = result.getInt(2);
        assertEquals(2000, i);
   }

    /**
     * Verify that the driver navigates a resultset according to the JDBC rules.
     * In all cases, the resultset should be pointing to the first record, which can
     * be read without invoking {@code next()}.
     * @throws Exception  Fatal error.
     */
    @Test
    public void testResultSetNavigation() throws Exception
    {
       Statement statement = con.createStatement();

       String truncate = "TRUNCATE regressiontest;";
       statement.execute(truncate);
       
       String insert1 = "INSERT INTO regressiontest (keyname,bValue,iValue) VALUES( 'key0',true, 2000);";
       statement.executeUpdate(insert1);
       
       String insert2 = "INSERT INTO regressiontest (keyname,bValue) VALUES( 'key1',false);";
       statement.executeUpdate(insert2);
       
       
       
       String select = "SELECT * from regressiontest;";
       
       ResultSet result = statement.executeQuery(select);
       
       ResultSetMetaData metadata = result.getMetaData();
       
       int colCount = metadata.getColumnCount();
       
       System.out.println("Before doing a next()");
       System.out.printf("(%d) ",result.getRow());
       for (int i = 1; i <= colCount; i++)
       {
           System.out.print(showColumn(i,result)+ " "); 
       }
       System.out.println();
       
       
       System.out.println("Fetching each row with a next()");
       while (result.next())
       {
           metadata = result.getMetaData();
           colCount = metadata.getColumnCount();
           System.out.printf("(%d) ",result.getRow());
           for (int i = 1; i <= colCount; i++)
           {
               System.out.print(showColumn(i,result)+ " "); 
           }
           System.out.println();
       }
    }

    /**
     * Create a column group and confirm that the {@code ResultSetMetaData} works.
     */
    @Test
    public void testResultSetMetaData() throws Exception {

        Statement stmt = con.createStatement();

        // Create the target Column family
        String createCF = "CREATE COLUMNFAMILY t33 (k int PRIMARY KEY,"
                + "c text "
                + ") ;";

        stmt.execute(createCF);
        stmt.close();
        con.close();

        // open it up again to see the new CF
        con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s?%s", HOST, PORT, KEYSPACE, OPTIONS));

        // paraphrase of the snippet from the ISSUE #33 provided test
        PreparedStatement statement = con.prepareStatement("update t33 set c=? where k=123");
        statement.setString(1, "mark");
        statement.executeUpdate();

        ResultSet result = statement.executeQuery("SELECT k, c FROM t33;");

        ResultSetMetaData metadata = result.getMetaData();

        int colCount = metadata.getColumnCount();

        System.out.println("Test Issue #33");
        DatabaseMetaData md = con.getMetaData();
        System.out.println();
        System.out.println("--------------");
        System.out.println("Driver Version :   " + md.getDriverVersion());
        System.out.println("DB Version     :   " + md.getDatabaseProductVersion());
        System.out.println("Catalog term   :   " + md.getCatalogTerm());
        System.out.println("Catalog        :   " + con.getCatalog());
        System.out.println("Schema term    :   " + md.getSchemaTerm());

        System.out.println("--------------");
        while (result.next()) {
            metadata = result.getMetaData();
            colCount = metadata.getColumnCount();

            assertEquals("Total column count should match schema for t33", 2, metadata.getColumnCount());

            System.out.printf("(%d) ", result.getRow());
            for (int i = 1; i <= colCount; i++) {
                System.out.print(showColumn(i, result) + " ");

                switch (i) {
                    case 1:
                        assertEquals("First Column: k", "k", metadata.getColumnName(1));
                        assertEquals("First Column Type: int", Types.INTEGER, metadata.getColumnType(1));
                        break;
                    case 2:
                        assertEquals("Second Column: c", "c", metadata.getColumnName(2));
                        assertEquals("Second Column Type: text", Types.NVARCHAR, metadata.getColumnType(2));
                        break;
                }
            }
            System.out.println();
        }
    }

    /**
     * Test the meta-data logic for the database. This should allow you to query the
     * schema information dynamically. Previously this was <i>Issue 40</i>.
     */
    @Test
    public void testDatabaseMetaData() throws Exception
    {
        DatabaseMetaData md = con.getMetaData();

        // test various retrieval methods
        ResultSet result = md.getTables(con.getCatalog(), null, "%", new String[]
        { "TABLE" });
        assertTrue("Make sure we have found a table", result.next());
        result = md.getTables(null, KEYSPACE, TABLE, null);
        assertTrue("Make sure we have found the table asked for", result.next());
        result = md.getTables(null, KEYSPACE, TABLE, new String[]
        { "TABLE" });
        assertTrue("Make sure we have found the table asked for", result.next());
        result = md.getTables(con.getCatalog(), KEYSPACE, TABLE, new String[]
        { "TABLE" });
        assertTrue("Make sure we have found the table asked for", result.next());

        // check the table name
        String tn = result.getString("TABLE_NAME");
        assertEquals("Table name match", TABLE, tn);
        System.out.println("Found table via dmd    :   " + tn);

        // load the columns
        result = md.getColumns(con.getCatalog(), KEYSPACE, TABLE, null);
        assertTrue("Make sure we have found first column", result.next());
        assertEquals("Make sure table name match", TABLE, result.getString("TABLE_NAME"));
        String cn = result.getString("COLUMN_NAME");
        System.out.println("Found (default) PK column       :   " + cn);
        assertEquals("Column name check", "keyname", cn);
        assertEquals("Column type check", Types.VARCHAR, result.getInt("DATA_TYPE"));
        assertTrue("Make sure we have found second column", result.next());
        cn = result.getString("COLUMN_NAME");
        System.out.println("Found column       :   " + cn);
        assertEquals("Column name check", "bvalue", cn);
        assertEquals("Column type check", Types.BOOLEAN, result.getInt("DATA_TYPE"));
        assertTrue("Make sure we have found thirth column", result.next());
        cn = result.getString("COLUMN_NAME");
        System.out.println("Found column       :   " + cn);
        assertEquals("Column name check", "ivalue", cn);
        assertEquals("Column type check", Types.INTEGER, result.getInt("DATA_TYPE"));

        // make sure we filter
        result = md.getColumns(con.getCatalog(), KEYSPACE, TABLE, "bvalue");
        result.next();
        assertFalse("Make sure we have found requested column only", result.next());
    }

    /**
     * Test a simple resultset. This technically also demonstrates the upsert functionality
     * of Cassandra.
     */
    @Test
    public void testSimpleResultSet() throws Exception
    {
        Statement stmt = con.createStatement();
        
        // Create the target Column family
        String createCF = "CREATE COLUMNFAMILY t59 (k int PRIMARY KEY," 
                        + "c text "
                        + ") ;";        
        
        stmt.execute(createCF);
        stmt.close();
        con.close();

        // open it up again to see the new CF
        con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s?%s",HOST,PORT,KEYSPACE,OPTIONS));
 
        PreparedStatement statement = con.prepareStatement("update t59 set c=? where k=123", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        statement.setString(1, "hello");
        statement.executeUpdate();

        ResultSet result = statement.executeQuery("SELECT * FROM t59;");
        
        System.out.println(resultToDisplay(result,59,null));

    }

    /**
     * Test the {@code Set} object in Cassandra.
     */
    @Test
    public void testObjectSet() throws Exception
    {
        Statement stmt = con.createStatement();
        
        // Create the target Column family
        String createCF = "CREATE COLUMNFAMILY t65 (key text PRIMARY KEY," 
                        + "int1 int, "
                        + "int2 int, "
                        + "intset  set<int> "
                        + ") ;";        
        
        stmt.execute(createCF);
        stmt.close();
        con.close();

        // open it up again to see the new CF
        con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s?%s",HOST,PORT,KEYSPACE,OPTIONS));
        
        Statement statement = con.createStatement();
        String insert = "INSERT INTO t65 (key, int1,int2,intset) VALUES ('key1',1,100,{10,20,30,40});";
        statement.executeUpdate(insert);
        
        ResultSet result = statement.executeQuery("SELECT intset FROM t65 where key = 'key1';");

        // read the Set of Integer back out again
        Object rsObject = result.getObject(1);
        assertNotNull("Object Should Exist", rsObject);
        assertTrue("Object Should be Set", (rsObject instanceof Set));

        int sum = 0;

        // sum up the set - it should be 100
        for (Object rsEntry : ((Set)rsObject).toArray()) {
            assertTrue("Entry should be Integer", (rsEntry instanceof Integer));
            sum += ((Integer)rsEntry).intValue();
        }

        assertEquals("Total Should be 100", 100, sum);

        //System.out.println(resultToDisplay(result,65, "with set = {10,20,30,40}"));
       
        String update = "UPDATE t65 SET intset=? WHERE key=?;";
 
        PreparedStatement pstatement = con.prepareStatement(update);
        Set<Integer> mySet = new HashSet<Integer> ();
        pstatement.setObject(1, mySet, Types.OTHER);
        pstatement.setString(2, "key1");
       
        pstatement.executeUpdate();

        result = statement.executeQuery("SELECT * FROM t65;");
        
        System.out.println(resultToDisplay(result,65," with set = <empty>"));

    }

    /**
     * Confirm that the connection can accept a different consistency level.
     */
    @Test
    public void testConsistencyLevel() throws Exception
    {
        Statement stmt = con.createStatement();
        
        // Create the target Column family
        String createCF = "CREATE COLUMNFAMILY t71 (k int PRIMARY KEY," 
                        + "c text "
                        + ") ;";        
        
        stmt.execute(createCF);
        stmt.close();
        con.close();

        // open it up again to see the new CF
       con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s?%s&consistency=%s",HOST,PORT,KEYSPACE,OPTIONS,CONSISTENCY_QUORUM));
      
       // at this point defaultConsistencyLevel should be set the QUORUM in the connection
       stmt = con.createStatement();
       
       ConsistencyLevel cl = statementExtras(stmt).getConsistencyLevel();
       assertTrue(ConsistencyLevel.QUORUM == cl );
       
       System.out.println();
       System.out.println("Test Issue #71");
       System.out.println("--------------");
       System.out.println("statement.consistencyLevel = "+ cl);
       


    }
    
    @Test
    public void testObjectTimestamp() throws Exception
    {
        Statement stmt = con.createStatement();
        java.util.Date now = new java.util.Date();

        
        // Create the target Column family
        //String createCF = "CREATE COLUMNFAMILY t74 (id BIGINT PRIMARY KEY, col1 TIMESTAMP)";        
        String createCF = "CREATE COLUMNFAMILY t74 (id BIGINT PRIMARY KEY, col1 TIMESTAMP)";
        
        stmt.execute(createCF);
        stmt.close();
        con.close();

        // open it up again to see the new CF
        con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s?%s",HOST,PORT,KEYSPACE,OPTIONS));
        
        Statement statement = con.createStatement();
        
        String insert = "INSERT INTO t74 (id, col1) VALUES (?, ?);";
        
        PreparedStatement pstatement = con.prepareStatement(insert);
        pstatement.setLong(1, 1L); 
        pstatement.setObject(2, new Timestamp(now.getTime()),Types.TIMESTAMP);
        pstatement.execute();

        ResultSet result = statement.executeQuery("SELECT * FROM t74;");
        
        assertTrue(result.next());
        assertEquals(1L, result.getLong(1));

        // try reading Timestamp directly
        Timestamp stamp = result.getTimestamp(2);
        assertEquals(now, stamp);

        // try reading Timestamp as an object
        stamp = result.getObject(2, Timestamp.class);
        assertEquals(now, stamp);

        System.out.println(resultToDisplay(result,74, "current date"));
       
    }

    /**
     * Verify that even with an empty ResultSet; the resultset meta-data can still
     * be queried. Previously, this was <i>Issue 75</i>.
     */
    @Test
    public void testEmptyResultSet() throws Exception
    {

        Statement stmt = con.createStatement();

        String truncate = "TRUNCATE regressiontest;";
        stmt.execute(truncate);

        String select = "select ivalue from "+TABLE;        
        
        ResultSet result = stmt.executeQuery(select);
        assertFalse("Make sure we have no rows", result.next());
        ResultSetMetaData rsmd = result.getMetaData();
        assertTrue("Make sure we do get a result", rsmd.getColumnDisplaySize(1) != 0);
        assertNotNull("Make sure we do get a label",rsmd.getColumnLabel(1));
        System.out.println("Found a column in ResultsetMetaData even when there are no rows:   " + rsmd.getColumnLabel(1));
        stmt.close();
    }

    /**
     * Previously this was <i>Issue 76</i>.
     */
    @Test
    public void testDatabaseMetaViaResultSet() throws Exception
    {
        DatabaseMetaData md = con.getMetaData();

        // test various retrieval methods
        ResultSet result = md.getIndexInfo(con.getCatalog(), KEYSPACE, TABLE, false, false);
        assertTrue("Make sure we have found an index", result.next());

        // check the column name from index
        String cn = result.getString("COLUMN_NAME");
        assertEquals("Column name match for index", "ivalue", cn);
        System.out.println("Found index via dmd on :   " + cn);
    }

    @Test
    public void testIssue77() throws Exception
    {
        DatabaseMetaData md = con.getMetaData();
        System.out.println();
        System.out.println("Test Issue #77");
        System.out.println("--------------");

        // test various retrieval methods
        ResultSet result = md.getPrimaryKeys(con.getCatalog(), KEYSPACE, TABLE);
        assertTrue("Make sure we have found an pk", result.next());

        // check the column name from index
        String cn = result.getString("COLUMN_NAME");
        assertEquals("Column name match for pk", "keyname", cn);
        System.out.println("Found pk via dmd :   " + cn);
    }

    @Test
    public void testIssue78() throws Exception
    {
        DatabaseMetaData md = con.getMetaData();

        // load the columns, with no catalog and schema
        ResultSet result = md.getColumns(null, "%", TABLE, "ivalue");
        assertTrue("Make sure we have found an column", result.next());
    }

    @Test
    public void testBlob() throws Exception
    {
        UUID blobId = UUID.randomUUID();
        CassandraBlob blobValue = new CassandraBlob(RandomStringUtils.random(10));
        String insert = "INSERT INTO " + TYPETABLE + " (id,blobValue,dataMapValue) " +
                        " VALUES(" + blobId.toString() + ", ?, {'12345': bigintAsBlob(12345)});";

        PreparedStatement statement = con.prepareStatement(insert);
        statement.setBlob(1, blobValue);

        statement.executeUpdate();
        statement.close();

        Statement select1 = con.createStatement();
        String query = "SELECT blobValue FROM "+TYPETABLE+" WHERE id=" + blobId.toString() + ";";
        ResultSet result = select1.executeQuery(query);
        result.next();

        Blob blobResult = result.getBlob(1);
        assertEquals(blobValue, blobResult);

        Statement select2 = con.createStatement();
        String query2 = "SELECT dataMapValue FROM "+TYPETABLE+" WHERE id=" + blobId.toString() + ";";
        ResultSet result2 = select2.executeQuery(query2);
        result2.next();

        Object mapResult = result2.getObject(1);

        assertTrue(mapResult instanceof Map);
        assertEquals("There should be 1 record in the map", 1, ((Map)mapResult).size());
        assertNotNull("Entry for '12345' should be in the map", ((Map)mapResult).get("12345"));
        assertNull("Entry for '54321' should NOT be in the map", ((Map)mapResult).get("54321"));

    }


    @Test
    public void isValid() throws Exception
    {
//    	assert con.isValid(3);
    }
    
    @Test(expected=SQLException.class)
    public void isValidSubZero() throws Exception
    {
    	con.isValid(-42);
    }
    
    @Test
    public void isNotValid() throws Exception
    {
//        PreparedStatement currentStatement = ((CassandraConnection) con).isAlive;
//        PreparedStatement mockedStatement = mock(PreparedStatement.class);
//        when(mockedStatement.executeQuery()).thenThrow(new SQLException("A mocked ERROR"));
//        ((CassandraConnection) con).isAlive = mockedStatement;
//        assert con.isValid(5) == false;
//        ((CassandraConnection) con).isAlive = currentStatement;
    }
    
    private String showColumn(int index, ResultSet result) throws SQLException
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(index).append("]");
        sb.append(result.getObject(index));
        return sb.toString();
    }
    
    private String resultToDisplay(ResultSet result, int issue, String note) throws Exception
    {
        StringBuilder sb = new StringBuilder("Test Issue #" + issue + " - "+ note + "\n");
       ResultSetMetaData metadata = result.getMetaData();
        
        int colCount = metadata.getColumnCount();
        
        sb.append("--------------").append("\n");
        while (result.next())
        {
            metadata = result.getMetaData();
            colCount = metadata.getColumnCount();
            sb.append(String.format("(%d) ",result.getRow()));
            for (int i = 1; i <= colCount; i++)
            {
                sb.append(showColumn(i,result)+ " "); 
            }
            sb.append("\n");
        }
        
        return sb.toString();        
    }
    
    private CassandraStatementExtras statementExtras(Statement statement) throws Exception
    {
        Class cse = Class.forName("CassandraStatementExtras");
        return (CassandraStatementExtras) statement.unwrap(cse);
    }

}
