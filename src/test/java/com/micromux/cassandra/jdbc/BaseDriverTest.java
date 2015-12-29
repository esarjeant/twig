package com.micromux.cassandra.jdbc;

import com.micromux.cassandra.ConnectionDetails;
import org.apache.commons.lang3.StringUtils;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.net.URLEncoder;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

/**
 * Base driver test harness; this is used as a common entry point for stating up the embedded Cassandra
 * instance and configuring the keyspaces.
 */
public class BaseDriverTest {

    static final String HOST = System.getProperty("host", ConnectionDetails.getHost());
    static final int PORT = Integer.parseInt(System.getProperty("port", ConnectionDetails.getPort()+""));
    static final String SYSTEM = "system";
    static final String KEYSPACE = "testks";
    static final String USER = "JohnDoe";
    static final String PASSWORD = "secret";
    static final String VERSION = "3.0.0";
    static final String CONSISTENCY = "ONE";

    // use these for encyrpted connections
    static final String TRUST_STORE = System.getProperty("trustStore");
    static final String TRUST_PASS = System.getProperty("trustPass", "cassandra");

    static String OPTIONS = "";

    static java.sql.Connection con = null;

    /**
     * This block is critical; it only needs to happen once but if it fails all tests will be in doubt.
     * An error is logged in the event that this does not succeed.
     */
    static {

        try {

            // start embedded Cassandra
            EmbeddedCassandraServerHelper.startEmbeddedCassandra("cu-cassandra.yaml", "build/embeddedCassandra");

            // configure OPTIONS
            if (!StringUtils.isEmpty(TRUST_STORE)) {
                OPTIONS = String.format("trustStore=%s&trustPass=%s", URLEncoder.encode(TRUST_STORE), TRUST_PASS);
            }

            // load the database driver
            Class.forName(CassandraDriver.class.getName());

        } catch (Exception ex) {
            System.err.println("Failed to start Embedded Cassandra");
            ex.printStackTrace(System.err);
        }

    }

    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {

        // connect to the database
        String urlSystem = createConnectionUrl(SYSTEM);
        con = DriverManager.getConnection(urlSystem);

        // Cleanup Keyspaces
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();

        // Create KeySpace
        String createKS = String.format("CREATE KEYSPACE \"%s\" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};",KEYSPACE);
        Statement stmt = null;

        try {
            stmt = con.createStatement();
            stmt.execute(createKS);
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }

    }


    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        try {
            if (con != null) con.close();
        } finally {
            EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
        }

    }

    static String createConnectionUrl(String keyspace) {
        return String.format("jdbc:cassandra://%s:%d/%s?%s&version=%s", HOST, PORT, keyspace, OPTIONS, VERSION);
    }

    static CassandraStatementExtras statementExtras(Statement statement) throws Exception
    {
        Class cse = Class.forName("com.micromux.cassandra.jdbc.CassandraStatementExtras");
        return (CassandraStatementExtras) statement.unwrap(cse);
    }

}
