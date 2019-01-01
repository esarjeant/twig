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

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.CharacterCodingException;
import java.sql.*;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;


public class MetadataResultSetsTest extends BaseDriverTest
{

    private static final String KEYSPACE1 = "testks1";
    private static final String KEYSPACE2 = "testks2";
    private static final String DROP_KS = "DROP KEYSPACE \"%s\";";
    private static final String CREATE_KS = "CREATE KEYSPACE \"%s\" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};";
      
    @Before
    public void setUpBeforeTest() throws Exception
    {

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
        
        String createEmployees = "CREATE TABLE employees (" +
                " empid int," +
                " deptid int," +
                " emails set<text>," +
                " first_name text," +
                " hire_date date," +
                " last_name text," +
                " PRIMARY KEY ((deptid, empid), first_name, last_name)" +
                " );";

        String createEmployeesByFirstNameView = "CREATE MATERIALIZED VIEW employees_by_name AS"
                        + " SELECT empid, deptid, emails, first_name, hire_date, last_name "
                        + " FROM employees "
                        + " WHERE empid IS NOT NULL "
                        + " AND deptid IS NOT NULL "
                        + " AND first_name IS NOT NULL "
                        + " AND last_name IS NOT NULL "
                        + " PRIMARY KEY ((deptid, first_name), empid, last_name);";

        stmt.execute(createCF1);
        stmt.execute(createCF2);
        stmt.execute(createEmployees);
        stmt.execute(createEmployeesByFirstNameView);

        stmt.execute(useKS2);
        stmt.execute(createCF1);
        stmt.execute(createCF2);

        stmt.close();
        con.close();

        // open it up again to see the new CF
        String url = createConnectionUrl(KEYSPACE1);
        con = DriverManager.getConnection(url);

    }

    private String showColumn(int index, ResultSet result) throws SQLException
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(index).append("]");
        sb.append(result.getObject(index));
        return sb.toString();
    }

    private String toString(ResultSet result) throws SQLException
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

	private String getColumnNames(ResultSetMetaData metaData) throws SQLException
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
        assertNotNull("Schema not found", result);

        Set<String> catalogNameSet = new HashSet<>();
        Set<String> schemaNameSet = new HashSet<>();

        while (result.next()) {
            catalogNameSet.add(result.getString("TABLE_CATALOG"));
            schemaNameSet.add(result.getString("TABLE_SCHEM"));
        }

        assertEquals("Catalog names should match", 1, catalogNameSet.size());
        assertEquals("Schema names should match", 8, schemaNameSet.size());

        assertTrue("Schema system exists", schemaNameSet.contains("system"));
        assertTrue("Schema system_schema exists", schemaNameSet.contains("system_schema"));
        assertTrue("Schema system_distributed exists", schemaNameSet.contains("system_distributed"));
        assertTrue("Schema system_traces exists", schemaNameSet.contains("system_traces"));
        assertTrue("Schema system_auth exists", schemaNameSet.contains("system_auth"));
        assertTrue("Schema testks exists", schemaNameSet.contains("testks"));
        assertTrue("Schema testks1 exists", schemaNameSet.contains("testks1"));
        assertTrue("Schema testks2 exists", schemaNameSet.contains("testks2"));

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
        ResultSet result = MetadataResultSets.makeTables(statement, null, null, Collections.<String>emptySet());

        System.out.println("--- testTables() ---");
        System.out.println(getColumnNames(result.getMetaData()));
       
        System.out.println(toString(result));       
        System.out.println();
        
        result = MetadataResultSets.makeTables(statement, KEYSPACE2, null, Collections.<String>emptySet());
        System.out.println(toString(result));       
        System.out.println();

        result = MetadataResultSets.makeTables(statement, null, "test1", Collections.<String>emptySet());
        System.out.println(toString(result));       
        System.out.println();

        result = MetadataResultSets.makeTables(statement, KEYSPACE2, "test1", Collections.<String>emptySet());
        System.out.println(toString(result));       
        System.out.println();
    }

    @Test
    public void testViews() throws SQLException
    {
        CassandraStatement statement = (CassandraStatement) con.createStatement();

        System.out.println("--- testViews() ---");
        ResultSet result = MetadataResultSets.makeTables(statement, KEYSPACE1, "employees", Sets.newHashSet("TABLE"));
        assertNotNull("View Employees Not Found", result);

        int totalEmployees = 0;

        while (result.next()) {
            totalEmployees++;
            assertEquals("Table schema should match", KEYSPACE1, result.getString("TABLE_CAT"));
            assertEquals("Table schema should match", KEYSPACE1, result.getString("TABLE_SCHEM"));

            if ("employees".equals(result.getString("TABLE_NAME"))) {
                assertEquals("Table type should match", "TABLE", result.getString("TABLE_TYPE"));
            }

        }

        assertEquals("There should be (1) entities matching employees", 1, totalEmployees);

        System.out.println(toString(result));
        System.out.println();

        result = MetadataResultSets.makeTables(statement, KEYSPACE1, "employees_by_name", Sets.newHashSet("VIEW"));
        assertNotNull("View EmployeesByName Not Found", result);

        int totalEmployeesByName = 0;

        while (result.next()) {
            totalEmployeesByName++;
            assertEquals("Table schema should match", KEYSPACE1, result.getString("TABLE_CAT"));
            assertEquals("Table schema should match", KEYSPACE1, result.getString("TABLE_SCHEM"));
            assertEquals("Table type should match", "VIEW", result.getString("TABLE_TYPE"));
            assertEquals("Table name should match", "employees_by_name", result.getString("TABLE_NAME"));
        }

        assertEquals("There should be (1) view employees_by_name", 1, totalEmployeesByName);

        System.out.println(toString(result));
        System.out.println();

    }

    @Test
    public void testColumns_test1() throws SQLException, CharacterCodingException {

        CassandraStatement statement = (CassandraStatement) con.createStatement();
        ResultSet result = MetadataResultSets.makeColumns(statement, KEYSPACE1, "test1" ,null);
        
        Set<String> columnNames = new HashSet<>();

        while (result.next()) {

            // verify base schema matches
            assertTrue("table catalog", StringUtils.startsWith(result.getString("TABLE_CAT"), "cluster"));
            assertEquals("table schema", "testks1", result.getString("TABLE_SCHEM"));
            assertEquals("table name", "test1", result.getString("TABLE_NAME"));

            // include column name
            columnNames.add(result.getString("COLUMN_NAME"));

            // verify datatype for keyname column
            if (StringUtils.equalsIgnoreCase("keyname", result.getString("COLUMN_NAME"))) {
                assertEquals("TYPE_NAME", "text", result.getString("TYPE_NAME"));
                assertEquals("DATA_TYPE", 12, result.getInt("DATA_TYPE"));
                assertEquals("COLUMN_SIZE", 25, result.getInt("COLUMN_SIZE"));
                assertEquals("ORDINAL_POSITION", 1, result.getInt("ORDINAL_POSITION"));
                assertEquals("NULLABLE", 1, result.getInt("NULLABLE"));
            }

            // verify datatype for t1bValue column
            if (StringUtils.equalsIgnoreCase("t1bValue", result.getString("COLUMN_NAME"))) {
                assertEquals("TYPE_NAME", "boolean", result.getString("TYPE_NAME"));
                assertEquals("DATA_TYPE", 16, result.getInt("DATA_TYPE"));
                assertEquals("COLUMN_SIZE", 1, result.getInt("COLUMN_SIZE"));
                assertEquals("ORDINAL_POSITION", 2, result.getInt("ORDINAL_POSITION"));
                assertEquals("NULLABLE", 1, result.getInt("NULLABLE"));
            }

            // verify datatype for t1iValue column
            if (StringUtils.equalsIgnoreCase("t1iValue", result.getString("COLUMN_NAME"))) {
                assertEquals("TYPE_NAME", "int", result.getString("TYPE_NAME"));
                assertEquals("DATA_TYPE", 4, result.getInt("DATA_TYPE"));
                assertEquals("COLUMN_SIZE", 8, result.getInt("COLUMN_SIZE"));
                assertEquals("ORDINAL_POSITION", 3, result.getInt("ORDINAL_POSITION"));
                assertEquals("NULLABLE", 1, result.getInt("NULLABLE"));
            }

        }

        assertEquals("There should be 3 columns", 3, columnNames.size());
        assertTrue("Column keyname not found", columnNames.contains("keyname"));
        assertTrue("Column t1ivalue not found", columnNames.contains("t1ivalue"));
        assertTrue("Column t1bvalue not found", columnNames.contains("t1bvalue"));

    }

    @Test
    public void testColumns_test2() throws SQLException, CharacterCodingException {

        CassandraStatement statement = (CassandraStatement) con.createStatement();
        ResultSet result = MetadataResultSets.makeColumns(statement, KEYSPACE1, "test2" ,null);

        Set<String> columnNames = new HashSet<>();

        while (result.next()) {

            // verify base schema matches
            assertTrue("table catalog", StringUtils.startsWith(result.getString("TABLE_CAT"), "cluster"));
            assertEquals("table schema", "testks1", result.getString("TABLE_SCHEM"));
            assertEquals("table name", "test2", result.getString("TABLE_NAME"));

            // include column name
            columnNames.add(result.getString("COLUMN_NAME"));

            // verify datatype for keyname column
            if (StringUtils.equalsIgnoreCase("keyname", result.getString("COLUMN_NAME"))) {
                assertEquals("TYPE_NAME", "text", result.getString("TYPE_NAME"));
                assertEquals("DATA_TYPE", 12, result.getInt("DATA_TYPE"));
                assertEquals("COLUMN_SIZE", 25, result.getInt("COLUMN_SIZE"));
                assertEquals("ORDINAL_POSITION", 1, result.getInt("ORDINAL_POSITION"));
                assertEquals("NULLABLE", 1, result.getInt("NULLABLE"));
            }

            // verify datatype for t2bValue column
            if (StringUtils.equalsIgnoreCase("t2bValue", result.getString("COLUMN_NAME"))) {
                assertEquals("TYPE_NAME", "boolean", result.getString("TYPE_NAME"));
                assertEquals("DATA_TYPE", 16, result.getInt("DATA_TYPE"));
                assertEquals("COLUMN_SIZE", 1, result.getInt("COLUMN_SIZE"));
                assertEquals("ORDINAL_POSITION", 2, result.getInt("ORDINAL_POSITION"));
                assertEquals("NULLABLE", 1, result.getInt("NULLABLE"));
            }

            // verify datatype for t1iValue column
            if (StringUtils.equalsIgnoreCase("t2iValue", result.getString("COLUMN_NAME"))) {
                assertEquals("TYPE_NAME", "int", result.getString("TYPE_NAME"));
                assertEquals("DATA_TYPE", 4, result.getInt("DATA_TYPE"));
                assertEquals("COLUMN_SIZE", 8, result.getInt("COLUMN_SIZE"));
                assertEquals("ORDINAL_POSITION", 3, result.getInt("ORDINAL_POSITION"));
                assertEquals("NULLABLE", 1, result.getInt("NULLABLE"));
            }

        }

        assertEquals("There should be 3 columns", 3, columnNames.size());
        assertTrue("Column keyname not found", columnNames.contains("keyname"));
        assertTrue("Column t2ivalue not found", columnNames.contains("t2ivalue"));
        assertTrue("Column t2bvalue not found", columnNames.contains("t2bvalue"));

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

    @Test
    public void testMakePrimaryKey() throws SQLException, CharacterCodingException {

        CassandraStatement statement = (CassandraStatement) con.createStatement();
        ResultSet result = MetadataResultSets.makePrimaryKeys(statement, KEYSPACE1, "employees");

        Set<String> primaryKeys = new HashSet<>();
        while (result.next()) {
            primaryKeys.add(result.getString("COLUMN_NAME"));
        }

        assertEquals("There should be 4 primary keys", 4, primaryKeys.size());
        assertTrue("PrimaryKey empid", primaryKeys.contains("empid"));
        assertTrue("PrimaryKey deptid", primaryKeys.contains("deptid"));
        assertTrue("PrimaryKey last_name", primaryKeys.contains("last_name"));
        assertTrue("PrimaryKey first_name", primaryKeys.contains("first_name"));

    }
}
