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
import java.sql.Statement;

import org.apache.cassandra.cql.ConnectionDetails;
import org.junit.BeforeClass;
import org.junit.Test;

public class SpashScreenTest
{
    private static java.sql.Connection con = null;

    @BeforeClass
    public static void waxOn() throws Exception
    {
        Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
        con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s",
            ConnectionDetails.getHost(),
            ConnectionDetails.getPort(),
            "JdbcTestKeyspace"));

        // Create the target Column family
        String create = "CREATE COLUMNFAMILY Test (KEY text PRIMARY KEY) WITH comparator = ascii AND default_validation = bigint;";
        Statement stmt = con.createStatement();
        stmt.execute(create);
        stmt.close();
        con.close();

        // open it up again to see the new CF
        con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s",
            ConnectionDetails.getHost(),
            ConnectionDetails.getPort(),
            "JdbcTestKeyspace"));
    }


    @Test
    public void test() throws Exception
    {
        String query = "UPDATE Test SET a=?, b=? WHERE KEY=?";
        PreparedStatement statement = con.prepareStatement(query);

        statement.setLong(1, 100);
        statement.setLong(2, 1000);
        statement.setString(3, "key0");

        statement.executeUpdate();

        statement.close();
    }
}
