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

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.cassandra.cql.ConnectionDetails;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class MetadataResultSetsTest
{
    private static final String HOST = System.getProperty("host", ConnectionDetails.getHost());
    private static final int PORT = Integer.parseInt(System.getProperty("port", ConnectionDetails.getPort()+""));
    private static final String KEYSPACE = "TestKS";
      
    private static java.sql.Connection con = null;
    

    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
        String URL = String.format("jdbc:cassandra://%s:%d/%s",HOST,PORT,"system");
        System.out.println("Connection URL = '"+URL +"'");
        
        con = DriverManager.getConnection(URL);
        Statement stmt = con.createStatement();
        
        // Drop Keyspace
        String dropKS = String.format("DROP KEYSPACE %s;",KEYSPACE);
        
        try { stmt.execute(dropKS);}
        catch (Exception e){/* Exception on DROP is OK */}

        // Create KeySpace
        String createKS = String.format("CREATE KEYSPACE %s WITH strategy_class = SimpleStrategy AND strategy_options:replication_factor = 1;",KEYSPACE);
        System.out.println("createKS = '"+createKS+"'");
        stmt = con.createStatement();
        stmt.execute("USE system;");
        stmt.execute(createKS);
        
        // Use Keyspace
        String useKS = String.format("USE %s;",KEYSPACE);
        stmt.execute(useKS);
        
        // Create the target Column family
        String createCF = "CREATE COLUMNFAMILY RegressionTest (keyname text PRIMARY KEY," 
                        + " bValue boolean,"
                        + " iValue int"
                        + ") WITH comparator = ascii AND default_validation = bigint;";
        
        
        stmt.execute(createCF);
        stmt.close();
        con.close();

        // open it up again to see the new CF
        con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s",HOST,PORT,KEYSPACE));
        System.out.println(con);

    }
    
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        if (con!=null) con.close();
    }

    private final String  showColumn(int index, ResultSet result) throws SQLException
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(index).append("]");
        sb.append(result.getObject(index));
        return sb.toString();
    }

    private final String showRow(ResultSet result) throws SQLException
    {
        StringBuilder sb = new StringBuilder();
        return sb.toString();
    }
    
    @Test
    public void testTableType() throws SQLException
    {
        CassandraStatement statement = (CassandraStatement) con.createStatement();
        ResultSet result = MetadataResultSets.makeTableTypes(statement);
        
        while (result.next())
        {
            ResultSetMetaData metadata = result.getMetaData();
            int colCount = metadata.getColumnCount();
            System.out.printf("(%d) ",result.getRow());
            for (int i = 1; i <= colCount; i++)
            {
                System.out.print(showColumn(i,result)+ " "); 
            }
            System.out.println();
        }

    }

}
