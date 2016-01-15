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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

import static com.micromux.cassandra.jdbc.Utils.*;

/**
 * The Class CassandraDriver.
 */
public class CassandraDriver implements Driver
{
    public static final int DVR_MAJOR_VERSION = 2;
    public static final int DVR_MINOR_VERSION = 1;
    public static final int DVR_PATCH_VERSION = 1;

    public static final String DVR_NAME = "Cassandra Twig JDBC Driver";

    private static final Logger logger = LoggerFactory.getLogger(CassandraDriver.class);

    static
    {
        // Register the CassandraDriver with DriverManager
        try
        {
            CassandraDriver driverInst = new CassandraDriver();
            DriverManager.registerDriver(driverInst);
        }
        catch (SQLException e)
        {
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * Method to validate whether provided connection url matches with pattern or not.
     */
    public boolean acceptsURL(String url) throws SQLException
    {
        return url.startsWith(PROTOCOL);
    }

    /**
     * Method to return connection instance for given connection url and connection props.
     */
    public Connection connect(String url, Properties props) throws SQLException
    {
        Properties finalProps;
        if (acceptsURL(url))
        {
            // parse the URL into a set of Properties
            finalProps = Utils.parseURL(url);

            // override any matching values in finalProps with values from props
            finalProps.putAll(props);

            if (logger.isDebugEnabled()) logger.debug("Final Properties to Connection: {}", finalProps);

            return new CassandraConnection(finalProps);
        }
        else
        {
            return null; // signal it is the wrong driver for this protocol:subprotocol
        }
    }

    /**
     * Returns default major version.
     */
    public int getMajorVersion()
    {
        return DVR_MAJOR_VERSION;
    }

    /**
     * Returns default minor version.
     */
    public int getMinorVersion()
    {
        return DVR_MINOR_VERSION;
    }

    /**
     * Returns default driver property info object.
     */
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties props) throws SQLException
    {
        if (props == null) props = new Properties();

        DriverPropertyInfo[] info = new DriverPropertyInfo[6];

        info[0] = new DriverPropertyInfo(TAG_USER, props.getProperty(TAG_USER));
        info[0].description = "The 'user' property";

        info[1] = new DriverPropertyInfo(TAG_PASSWORD, props.getProperty(TAG_PASSWORD));
        info[1].description = "The 'password' property";

        info[2] = new DriverPropertyInfo(TAG_TRUST_STORE, props.getProperty(TAG_TRUST_STORE));
        info[2].description = "File path containing certificate for encyrpted communication (create with Java keytool)";

        info[3] = new DriverPropertyInfo(TAG_TRUST_PASSWORD, props.getProperty(TAG_TRUST_PASSWORD));
        info[3].description = "Password for the trust store";

        info[4] = new DriverPropertyInfo(TAG_SSL_ENABLE, props.getProperty(TAG_SSL_ENABLE));
        info[4].description = "Enable SSL communication";
        info[5].choices = new String[2];
        info[5].choices[0] = "true";
        info[5].choices[1] = "false";
        info[5].value = "false";

        info[5] = new DriverPropertyInfo(TAG_INTELLIJ_QUIRKS, props.getProperty(TAG_INTELLIJ_QUIRKS));
        info[5].description = "Enable special optimizations for IntelliJ";
        info[5].choices = new String[2];
        info[5].choices[0] = "true";
        info[5].choices[1] = "false";
        info[5].value = "false";

        info[6] = new DriverPropertyInfo(TAG_DBVIS_QUIRKS, props.getProperty(TAG_DBVIS_QUIRKS));
        info[6].description = "Enable special optimizations for DbVisualizer";
        info[6].choices = new String[2];
        info[6].choices[0] = "true";
        info[6].choices[1] = "false";
        info[6].value = "false";

        return info;
    }

    /**
     * Returns true, if it is jdbc compliant.
     */
    public boolean jdbcCompliant()
    {
        return false;
    }
    
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException
    {
    	throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }
}
