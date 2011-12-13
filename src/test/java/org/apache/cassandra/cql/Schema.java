package org.apache.cassandra.cql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
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
import java.sql.PreparedStatement;

public class Schema
{
    public static final String KEYSPACE_NAME = "JdbcTestKeyspace";

    private static final String createKeyspace = String.format("CREATE KEYSPACE %s WITH " + 
            "strategy_class = SimpleStrategy AND strategy_options:replication_factor = 1", KEYSPACE_NAME);
    private static final String[] createColumnFamilies = {
        "CREATE COLUMNFAMILY JdbcInteger0 (KEY blob PRIMARY KEY, 42 text) WITH comparator = varint " + 
                "AND default_validation = varint",
        "CREATE COLUMNFAMILY JdbcInteger1 (id text PRIMARY KEY, 99 text) WITH comparator = varint " +
                "AND default_validation = text",
        "CREATE COLUMNFAMILY JdbcUtf8    (KEY blob PRIMARY KEY, fortytwo varint) WITH comparator = text",
        "CREATE COLUMNFAMILY JdbcLong    (KEY blob PRIMARY KEY) WITH comparator = bigint AND default_validation = bigint",
        "CREATE COLUMNFAMILY JdbcBytes   (KEY blob PRIMARY KEY) WITH comparator = blob AND default_validation = blob",
        "CREATE COLUMNFAMILY JdbcAscii   (KEY blob PRIMARY KEY) WITH comparator = ascii AND default_validation = ascii",
        "CREATE COLUMNFAMILY Standard1   (KEY blob PRIMARY KEY) WITH comparator = blob AND default_validation = blob"
    };
    private Connection conn = null;

    public Schema(String host, int port)
    {
        try {
            Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
            conn = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/system", host, port));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void createSchema() throws SQLException
    {
        try
        {
            dropSchema();
        }
        catch (SQLSyntaxErrorException sqlerr)
        {
            // Keyspace doesn't exist, probably, but that's OK.
        }
        
        executeNoResults(conn, createKeyspace);
        executeNoResults(conn, String.format("USE %s", KEYSPACE_NAME));

        for (String create : createColumnFamilies)
            executeNoResults(conn, create);
    }

    public void dropSchema() throws SQLException
    {
        executeNoResults(conn, String.format("DROP KEYSPACE %s", KEYSPACE_NAME));
    }

    /** executes a prepared statement */
    private static void executeNoResults(final Connection con, final String cql) throws SQLException
    {
        PreparedStatement statement = con.prepareStatement(cql);
        statement.execute();
    }
}
