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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLSyntaxErrorException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * A set of static utility methods used by the JDBC Suite, and various default values and error message strings
 * that can be shared across classes.
 */
class Utils
{
    private static final Pattern KEYSPACE_PATTERN = Pattern.compile("USE (\\w+);?", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
    private static final Pattern SELECT_PATTERN = Pattern.compile("(?:SELECT|DELETE)\\s+.+\\s+FROM\\s+(\\w+).*", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
    private static final Pattern UPDATE_PATTERN = Pattern.compile("UPDATE\\s+(\\w+)\\s+.*", Pattern.CASE_INSENSITIVE);

    public static final String PROTOCOL = "jdbc:cassandra:";
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 9042;
    public static final ConsistencyLevel DEFAULT_CONSISTENCY = ConsistencyLevel.ONE;
    

    public static final String KEY_VERSION = "version";
    public static final String KEY_CONSISTENCY = "consistency";
    public static final String KEY_PRIMARY_DC = "primarydc";
    public static final String KEY_BACKUP_DC = "backupdc";
    public static final String KEY_CONNECTION_RETRIES = "retries";
    
    public static final String TAG_DESCRIPTION = "description";
    public static final String TAG_USER = "user";
    public static final String TAG_PASSWORD = "password";
    public static final String TAG_DATABASE_NAME = "databaseName";
    public static final String TAG_SERVER_NAME = "serverName";
    public static final String TAG_PORT_NUMBER = "portNumber";
    public static final String TAG_ACTIVE_CQL_VERSION = "activeCqlVersion";
    public static final String TAG_CQL_VERSION = "cqlVersion";
    public static final String TAG_BUILD_VERSION = "buildVersion";
    public static final String TAG_THRIFT_VERSION = "thriftVersion";
    public static final String TAG_CONSISTENCY_LEVEL = "consistencyLevel";
    public static final String TAG_INTELLIJ_QUIRKS = "intellijQuirks";
    public static final String TAG_DBVIS_QUIRKS = "dbvisjQuirks";

    public static final String TAG_TRUST_STORE = "ssltruststore";
    public static final String TAG_TRUST_PASSWORD = "ssltrustpass";
    public static final String TAG_SSL_ENABLE = "sslenable";

    public static final String TAG_PRIMARY_DC = "primaryDatacenter";
    public static final String TAG_BACKUP_DC = "backupDatacenter";
    public static final String TAG_CONNECTION_RETRIES = "retries";

    protected static final String WAS_CLOSED_CON = "method was called on a closed Connection";
    protected static final String WAS_CLOSED_STMT = "method was called on a closed Statement";
    protected static final String WAS_CLOSED_RSLT = "method was called on a closed ResultSet";
    protected static final String NO_INTERFACE = "no object was found that matched the provided interface: %s";
    protected static final String NO_TRANSACTIONS = "the Cassandra implementation does not support transactions";
    protected static final String NO_SERVER = "no Cassandra server is available";
    protected static final String ALWAYS_AUTOCOMMIT = "the Cassandra implementation is always in auto-commit mode";
    protected static final String BAD_TIMEOUT = "the timeout value was less than zero";
    protected static final String SCHEMA_MISMATCH = "schema does not match across nodes, (try again later)";
    protected static final String NOT_SUPPORTED = "the Cassandra implementation does not support this method";
    protected static final String NO_GEN_KEYS = "the Cassandra implementation does not currently support returning generated  keys";
    protected static final String NO_BATCH = "the Cassandra implementation does not currently support this batch in Statement";
    protected static final String NO_MULTIPLE = "the Cassandra implementation does not currently support multiple open Result Sets";
    protected static final String NO_VALIDATOR = "Could not find key validator for: %s.%s";
    protected static final String NO_COMPARATOR = "Could not find key comparator for: %s.%s";
    protected static final String NO_RESULTSET = "No ResultSet returned from the CQL statement passed in an 'executeQuery()' method";
    protected static final String NO_UPDATE_COUNT = "No Update Count was returned from the CQL statement passed in an 'executeUpdate()' method";
    protected static final String NO_CF = "no column family reference could be extracted from the provided CQL statement";
    protected static final String BAD_KEEP_RSET = "the argument for keeping the current result set : %s is not a valid value";
    protected static final String BAD_TYPE_RSET = "the argument for result set type : %s is not a valid value";
    protected static final String BAD_CONCUR_RSET = "the argument for result set concurrency : %s is not a valid value";
    protected static final String BAD_HOLD_RSET = "the argument for result set holdability : %s is not a valid value";
    protected static final String BAD_FETCH_DIR = "fetch direction value of : %s is illegal";
    protected static final String BAD_AUTO_GEN = "auto key generation value of : %s is illegal";
    protected static final String BAD_FETCH_SIZE = "fetch size of : %s rows may not be negative";
    protected static final String MUST_BE_POSITIVE = "index must be a positive number less or equal the count of returned columns: %s";
    protected static final String VALID_LABELS = "name provided was not in the list of valid column labels: %s";
    protected static final String NOT_TRANSLATABLE = "column was stored in %s format which is not translatable to %s";
    protected static final String NOT_BOOLEAN = "string value was neither 'true' nor 'false' :  %s";
    protected static final String HOST_IN_URL = "Connection url must specify a host, e.g., jdbc:cassandra://localhost:9170/Keyspace1";
    protected static final String HOST_REQUIRED = "a 'host' name is required to build a Connection";
    protected static final String BAD_KEYSPACE = "Keyspace names must be composed of alphanumerics and underscores (parsed: '%s')";
    protected static final String URI_IS_SIMPLE = "Connection url may only include host, port, and keyspace, consistency and version option, e.g., jdbc:cassandra://localhost:9170/Keyspace1?version=3.0.0&consistency=ONE";
    protected static final String NOT_OPTION = "Connection url only supports the 'version' and 'consistency' options";
    protected static final String FORWARD_ONLY = "Can not position cursor with a type of TYPE_FORWARD_ONLY";

    protected static final Logger logger = LoggerFactory.getLogger(Utils.class);

    /**
     * Parse a URL for the Cassandra JDBC Driver
     * <p/>
     * The URL must start with the Protocol: "jdbc:cassandra:"
     * The URI part(the "Subname") must contain a host and an optional port and optional keyspace name
     * ie. "//localhost:9042/Test1"
     *
     * @param url The full JDBC URL to be parsed
     * @return A list of properties that were parsed from the Subname
     * @throws SQLException
     */
    public static Properties parseURL(String url) throws SQLException
    {
        Properties props = new Properties();

        if (!(url == null))
        {
            props.setProperty(TAG_PORT_NUMBER, "" + DEFAULT_PORT);
            String rawUri = url.substring(PROTOCOL.length());
            URI uri;
            try
            {
                uri = new URI(rawUri);
            }
            catch (URISyntaxException e)
            {
                throw new SQLSyntaxErrorException(e);
            }

            String host = uri.getHost();
            if (host == null) throw new SQLNonTransientConnectionException(HOST_IN_URL);
            props.setProperty(TAG_SERVER_NAME, host);

            int port = uri.getPort() >= 0 ? uri.getPort() : DEFAULT_PORT;
            props.setProperty(TAG_PORT_NUMBER, "" + port);

            String keyspace = uri.getPath();
            if ((keyspace != null) && (!keyspace.isEmpty()))
            {
                if (keyspace.startsWith("/")) keyspace = keyspace.substring(1);
                if (!keyspace.matches("[a-zA-Z]\\w+"))
                    throw new SQLNonTransientConnectionException(String.format(BAD_KEYSPACE, keyspace));
                props.setProperty(TAG_DATABASE_NAME, keyspace);
            }

            if (uri.getUserInfo() != null)
                throw new SQLNonTransientConnectionException(URI_IS_SIMPLE);
            
            String query = uri.getQuery();
            if ((query != null) && (!query.isEmpty()))
            {
                Map<String,String> params = parseQueryPart(query);
                if (params.containsKey(KEY_VERSION) )
                {
                    props.setProperty(TAG_CQL_VERSION,params.get(KEY_VERSION));
                }                
                if (params.containsKey(KEY_CONSISTENCY) )
                {
                    props.setProperty(TAG_CONSISTENCY_LEVEL,params.get(KEY_CONSISTENCY));
                }
                if (params.containsKey(KEY_PRIMARY_DC) )
                {
                    props.setProperty(TAG_PRIMARY_DC,params.get(KEY_PRIMARY_DC));
                }
                if (params.containsKey(KEY_BACKUP_DC) )
                {
                    props.setProperty(TAG_BACKUP_DC,params.get(KEY_BACKUP_DC));
                }
                if (params.containsKey(KEY_CONNECTION_RETRIES) )
                {
                    props.setProperty(TAG_CONNECTION_RETRIES,params.get(KEY_CONNECTION_RETRIES));
                }
                if (params.containsKey(TAG_TRUST_STORE)) {

                    props.setProperty(TAG_TRUST_STORE, params.get(TAG_TRUST_STORE));
                }
                if (params.containsKey(TAG_TRUST_PASSWORD)) {
                    props.setProperty(TAG_TRUST_PASSWORD, params.get(TAG_TRUST_PASSWORD));
                }
                if (params.containsKey(TAG_INTELLIJ_QUIRKS)) {
                    props.setProperty(TAG_INTELLIJ_QUIRKS, params.get(TAG_INTELLIJ_QUIRKS));
                }

//               String[] items = query.split("&");
//               if (items.length != 1) throw new SQLNonTransientConnectionException(URI_IS_SIMPLE);
//               
//               String[] option = query.split("=");
//               if (!option[0].equalsIgnoreCase("version")) throw new SQLNonTransientConnectionException(NOT_OPTION);
//               if (option.length!=2) throw new SQLNonTransientConnectionException(NOT_OPTION);
//               props.setProperty(TAG_CQL_VERSION, option[1]);
            }
        }

        if (logger.isTraceEnabled()) logger.trace("URL : '{}' parses to: {}", url, props);

        return props;
    }

    /**
     * Create a "Subname" portion of a JDBC URL from properties.
     * 
     * @param props A Properties file containing all the properties to be considered.
     * @return A constructed "Subname" portion of a JDBC URL in the form of a CLI (ie: //myhost:9042/Test1?version=3.0.0 )
     * @throws SQLException
     */
    public static String createSubName(Properties props)throws SQLException
    {
        // make keyspace always start with a "/" for URI
        String keyspace = props.getProperty(TAG_DATABASE_NAME);
     
        // if keyspace is null then do not bother ...
        if (keyspace != null) 
            if (!keyspace.startsWith("/")) keyspace = "/"  + keyspace;
        
        String host = props.getProperty(TAG_SERVER_NAME);
        if (host==null)throw new SQLNonTransientConnectionException(HOST_REQUIRED);
                
        // construct a valid URI from parts... 
        URI uri;
        try
        {
            uri = new URI(
                null,
                null,
                host,
                props.getProperty(TAG_PORT_NUMBER)==null ? DEFAULT_PORT : Integer.parseInt(props.getProperty(TAG_PORT_NUMBER)),
                keyspace,
                makeQueryString(props),
                null);
        }
        catch (Exception e)
        {
            throw new SQLNonTransientConnectionException(e);
        }
        
        if (logger.isTraceEnabled()) logger.trace("Subname : '{}' created from : {}",uri.toString(), props);
        
        return uri.toString();
    }

    protected static String makeQueryString(Properties props)
    {
        StringBuilder sb = new StringBuilder();
        String version = (props.getProperty(TAG_CQL_VERSION));
        String consistency = (props.getProperty(TAG_CONSISTENCY_LEVEL));
        if (consistency!=null) sb.append(KEY_CONSISTENCY).append("=").append(consistency);
        if (version!=null)
        {
            if (sb.length() != 0) sb.append("&");
            sb.append(KEY_VERSION).append("=").append(version);
        }
        
        return (sb.length()==0) ? null : sb.toString().trim();
    }
    
    protected static Map<String,String> parseQueryPart(String query) throws SQLException
    {
        Map<String,String> params = new HashMap<String,String>();
        for (String param : query.split("&"))
        {
            try
            {
                String pair[] = param.split("=");
                String key = URLDecoder.decode(pair[0], "UTF-8").toLowerCase();
                String value = "";
                if (pair.length > 1) value = URLDecoder.decode(pair[1], "UTF-8"); 
                params.put(key, value);
            }
            catch (UnsupportedEncodingException e)
            {
                throw new SQLSyntaxErrorException(e);
            }
        }
        return params;
    }
}
