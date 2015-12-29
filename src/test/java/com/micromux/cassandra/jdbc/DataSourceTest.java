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

import org.junit.Test;

import javax.sql.DataSource;
import java.sql.SQLFeatureNotSupportedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DataSourceTest extends BaseDriverTest
{

    @Test
    public void testConstructor() throws Exception
    {
        CassandraDataSource cds = new CassandraDataSource(HOST,PORT,KEYSPACE,USER,PASSWORD,VERSION,CONSISTENCY,TRUST_STORE,TRUST_PASS);
        assertEquals(HOST,cds.getServerName());
        assertEquals(PORT,cds.getPortNumber());
        assertEquals(KEYSPACE,cds.getDatabaseName());
        assertEquals(USER,cds.getUser());
        assertEquals(PASSWORD,cds.getPassword());
        assertEquals(VERSION, cds.getVersion());
        
        DataSource ds = new CassandraDataSource(HOST,PORT,KEYSPACE,USER,PASSWORD,VERSION,CONSISTENCY,TRUST_STORE,TRUST_PASS);
        assertNotNull(ds);
        
        // null username and password
        java.sql.Connection cnx = ds.getConnection(null, null);
        assertFalse(cnx.isClosed());
        ds.setLoginTimeout(5);
        assertEquals(5, ds.getLoginTimeout());

        // no username and password
        cnx = ds.getConnection();
        assertFalse(cnx.isClosed());
        ds.setLoginTimeout(5);
        assertEquals(VERSION, ((CassandraConnection) cnx).getConnectionProps().get(Utils.TAG_CQL_VERSION));
        assertEquals(5, ds.getLoginTimeout());
    }

    
    @Test
    public void testIsWrapperFor() throws Exception
    {
        DataSource ds = new CassandraDataSource(HOST,PORT,KEYSPACE,USER,PASSWORD,VERSION,CONSISTENCY,TRUST_STORE,TRUST_PASS);
        
        boolean isIt = false;
                
        // it is a wrapper for DataSource
        isIt = ds.isWrapperFor(DataSource.class);        
        assertTrue(isIt);
        
        // it is not a wrapper for this test class
        isIt = ds.isWrapperFor(this.getClass());        
        assertFalse(isIt);
    }
 
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testUnwrap() throws Exception
    {
        DataSource ds = new CassandraDataSource(HOST,PORT,KEYSPACE,USER,PASSWORD,VERSION,CONSISTENCY,TRUST_STORE,TRUST_PASS);

        // it is a wrapper for DataSource
        DataSource newds = ds.unwrap(DataSource.class);        
        assertNotNull(newds);
        
        // it is not a wrapper for this test class
        newds = (DataSource) ds.unwrap(this.getClass());        
        assertNotNull(newds);
    }
}
