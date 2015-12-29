package com.micromux.cassandra;

import com.google.common.io.Files;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

/**
 * Load the connection details from the properties file.
 */
public final class ConnectionDetails {

    private static final ConnectionDetails INSTANCE = new ConnectionDetails();

    private final String host;

    private final int port;

    private ConnectionDetails() {
        Properties p = new Properties();
        InputStream stream = getClass().getResourceAsStream(getClass().getSimpleName() + ".properties");
        try {
            p.load(stream);
        } catch (Exception e) {
            // ignore, we'll use the defaults
        } finally {
            try {
                stream.close();
            } catch (Exception e) {
                // ignore
            }
        }
        
        host = p.getProperty("host", "localhost");
        int port;
        try {
            port = Integer.parseInt(p.getProperty("port", "9042"));
        } catch (NumberFormatException e) {
            port = 9042;
        }
        this.port = port;
    }

    public static String getHost() {
        return INSTANCE.host;
    }

    public static int getPort() {
        return INSTANCE.port;
    }
}
