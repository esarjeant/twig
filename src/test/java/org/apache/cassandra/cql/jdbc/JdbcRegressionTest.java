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

import static org.junit.Assert.*;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.cassandra.cql.ConnectionDetails;
import org.junit.BeforeClass;
import org.junit.Test;

public class JdbcRegressionTest
{
    private static java.sql.Connection con = null;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
        con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s",
            ConnectionDetails.getHost(),
            ConnectionDetails.getPort(),
            "JdbcTestKeyspace"));
        Statement stmt = con.createStatement();
        
        // Create KeySpace
        String createKS = "CREATE KEYSPACE 'JdbcTestKeyspace' WITH "
                        + "strategy_class = SimpleStrategy AND strategy_options:replication_factor = 1;";
        stmt.execute(createKS);

        // Create the target Column family
        String createCF = "CREATE COLUMNFAMILY RegressionTest (KEY text PRIMARY KEY," 
                        + "bValue boolean, "
                        + "iValue int "
                        + ") WITH comparator = ascii AND default_validation = bigint;";
        
        
        stmt.execute(createCF);
        stmt.close();
        con.close();

        // open it up again to see the new CF
        con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s",
            ConnectionDetails.getHost(),
            ConnectionDetails.getPort(),
            "JdbcTestKeyspace"));

    }

    @Test
    public void testIssue10() throws Exception
    {
        String insert = "INSERT INTO RegressionTest (KEY,bValue,iValue) VALUES( 'key0',true, 2000);";
        Statement statement = con.createStatement();

        statement.executeUpdate(insert);
        statement.close();
        
        Thread.sleep(3000);
        
        statement = con.createStatement();
        ResultSet result = statement.executeQuery("SELECT bValue,notThere,iValue FROM RegressionTest WHERE KEY=key0;");
        result.next();
        
        boolean b = result.getBoolean(1);
        System.out.println("b = "+ b);
        assertTrue(b);
        
        long l = result.getLong("notThere");
        assertEquals(0,l);
        System.out.println("l = "+ l + " ... wasNull() = "+ result.wasNull());
        
        int i = result.getInt(3);
        System.out.println("i ="+ i + " ... wasNull() = "+ result.wasNull());
        assertEquals(2000, i);
   }

}
