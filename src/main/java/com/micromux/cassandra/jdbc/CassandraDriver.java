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

import java.sql.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

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

    private static final Logger logger = Utils.getLogger();

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

            logger.log(Level.FINE, String.format("Final Properties to Connection: %s", finalProps.toString()));

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
     * Driver properties for Cassandra. Includes the options that are configurable for this driver; for example,
     * {@link Utils#TAG_SSL_ENABLE} can be used to allow secure communication.
     */
    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties props) throws SQLException
    {
        if (props == null) props = new Properties();

        int mp = 11;
        DriverPropertyInfo[] info = new DriverPropertyInfo[mp];

        info[--mp] = new DriverPropertyInfo(TAG_USER, props.getProperty(TAG_USER));
        info[mp].description = "The 'user' property";

        info[--mp] = new DriverPropertyInfo(TAG_PASSWORD, props.getProperty(TAG_PASSWORD));
        info[mp].description = "The 'password' property";

        info[--mp] = new DriverPropertyInfo(TAG_TRUST_STORE, props.getProperty(TAG_TRUST_STORE));
        info[mp].description = "File path containing certificate for encyrpted communication (create with Java keytool)";

        info[--mp] = new DriverPropertyInfo(TAG_TRUST_TYPE, props.getProperty(TAG_TRUST_TYPE));
        info[mp].description = "Trust store encoding";
        info[mp].value= "JKS";

        info[--mp] = new DriverPropertyInfo(TAG_TRUST_PASSWORD, props.getProperty(TAG_TRUST_PASSWORD));
        info[mp].description = "Password for the trust store";

        info[--mp] = new DriverPropertyInfo(TAG_SSL_ENABLE, props.getProperty(TAG_SSL_ENABLE));
        info[mp].description = "Enable SSL communication";
        info[mp].choices = new String[2];
        info[mp].choices[0] = "true";
        info[mp].choices[1] = "false";
        info[mp].value = "false";

        info[--mp] = new DriverPropertyInfo(TAG_INTELLIJ_QUIRKS, props.getProperty(TAG_INTELLIJ_QUIRKS));
        info[mp].description = "Enable special optimizations for IntelliJ";
        info[mp].choices = new String[2];
        info[mp].choices[0] = "true";
        info[mp].choices[1] = "false";
        info[mp].value = "false";

        info[--mp] = new DriverPropertyInfo(TAG_DBVIS_QUIRKS, props.getProperty(TAG_DBVIS_QUIRKS));
        info[mp].description = "Enable special optimizations for DbVisualizer";
        info[mp].choices = new String[2];
        info[mp].choices[0] = "true";
        info[mp].choices[1] = "false";
        info[mp].value = "false";

        info[--mp] = new DriverPropertyInfo(TAG_CONSISTENCY_LEVEL, props.getProperty(TAG_CONSISTENCY_LEVEL));
        info[mp].description = "Consistency level for accessing data";
        info[mp].choices = new String[ConsistencyLevel.values().length];

        int level = 0;
        for (ConsistencyLevel cl : ConsistencyLevel.values()) {
            info[mp].choices[level++] = cl.name();
        }

        info[mp].value = ConsistencyLevel.ONE.name();

        info[--mp] = new DriverPropertyInfo(TAG_LOG_ENABLE, props.getProperty(TAG_LOG_ENABLE));
        info[mp].description = "Enable logging to the specified logPath location";
        info[mp].choices = new String[2];
        info[mp].choices[0] = "true";
        info[mp].choices[1] = "false";
        info[mp].value = "false";

        info[--mp] = new DriverPropertyInfo(TAG_LOG_PATH, props.getProperty(TAG_LOG_PATH));
        info[mp].description = "File for logging CQL statements";

        return info;

    }

    /**
     * Returns true, if it is jdbc compliant.
     */
    @Override
    public boolean jdbcCompliant()
    {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException
    {
    	return logger.getParent();
    }
}
