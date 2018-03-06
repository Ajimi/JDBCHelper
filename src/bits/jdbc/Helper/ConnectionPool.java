package bits.jdbc.Helper;

import java.util.List;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * Manages JDBC connections to the database.
 *
 * @author Jeff S Smith
 */
public class ConnectionPool {
    /**
     * Connection pool.
     */
    List<PooledConnection> conPool;
    /**
     * Database driver name.
     */
    private String driverName;
    /**
     * Database connection URL.
     */
    private String conURL;
    /**
     * Database connection user name.
     */
    private String username;
    /**
     * Database connection password
     */
    private String password;
    /**
     * DatabaseType (e.g. DatabaseType.SQLITE).
     */
    private int dbType;

    /**
     * Constructor creates a JDBC connection using given parameters.
     */
    public ConnectionPool(int numPooledCon,
                          String driverName,
                          String conURL,
                          String username,
                          String password) {
        this.dbType = DatabaseType.getDbType(driverName);

        this.driverName = driverName;
        this.conURL = conURL;
        this.username = username;
        this.password = password;
        conPool = new ArrayList<>();
        addConnectionsToPool(numPooledCon);
    }

    /**
     * Constructor uses the given connection (con) as its connection. Use this constructor if you
     * want to get your connections from some custom code or from a connection pool.
     */
    public ConnectionPool(Connection conn) {
        conPool = new ArrayList<>();
        PooledConnection pc = new PooledConnection(conn, true);
        conPool.add(pc);
        this.dbType = DatabaseType.getDbType(conn);
    }

    /**
     * Creates database connection(s) and adds them to the pool.
     */
    private void addConnectionsToPool(int numPooledCon) {
        try {
            Class.forName(driverName).newInstance();
            for (int i = 0; i < numPooledCon; i++) {
                Connection con = DriverManager.getConnection(conURL, username, password);
                PooledConnection pc = new PooledConnection(con, true);
                conPool.add(pc);
            }
        } catch (Exception e) {
            System.err.println("Exception: add connections to pool");
            e.printStackTrace();
        }
    }

    /**
     * Gets the number of connections in the pool.
     */
    public int getNumConInPool() {
        return (conPool.size());
    }

    private void removeAnyClosedConnections() {
        try {
            boolean done = false;
            while (!done) {
                done = true;

                for (int i = 0; i < conPool.size(); i++) {
                    PooledConnection pc = (PooledConnection) conPool.get(i);
                    if (pc.getConnection().isClosed())  //remove any closed connections
                    {
                        conPool.remove(i);
                        done = false;
                        break;
                    }
                }
            }
        } catch (SQLException e) {
            System.err.println("Exception: remove any closed connections");
            e.printStackTrace();
        }
    }

    /**
     * Gets available connection from the pool
     *
     * @return Connection
     */
    public Connection getConnection() {
        //if any connections have been closed, remove them from the pool before we get the
        //next available connection
        removeAnyClosedConnections();

        for (int i = 0; i < conPool.size(); i++) {
            PooledConnection pc = (PooledConnection) conPool.get(i);
            if (pc.isAvailable()) {
                pc.setAvailable(false);
                return (pc.getConnection());
            }
        }

        //didn't find a connection, so add one to the pool
        addConnectionsToPool(1);
        PooledConnection pc = (PooledConnection) conPool.get(conPool.size() - 1);
        pc.setAvailable(false);
        return (pc.getConnection());
    }

    /**
     * Get the dbType
     *
     * @return int
     */
    public int getDbType() {
        return (dbType);
    }

    /**
     * Closes all connections in the connection pool.
     */
    public void closeAllConnections() {
        for (int i = 0; i < conPool.size(); i++) {
            PooledConnection pc = (PooledConnection) conPool.get(i);
            closeConnection(pc.getConnection());
        }

        conPool.clear();  //remove all PooledConnections from list
    }

    /**
     * Attempts to resize the connection pool to the new size. This method will not free any
     * connections which are not available (in use)--so it may not resize the pool. It will always
     * enlarge the connection pool if newSize > current size.
     *
     * @param newSize
     * @return int new size of connection pool
     */
    public int resizeConnectionPool(int newSize) throws SQLException {
        if ((newSize < 0) || (newSize > 999))
            throw new SQLException("Connection pool size must be between 0 and 999");

        removeAnyClosedConnections();

        if (newSize > conPool.size())  //add new connections to pool
        {
            int conToAdd = (newSize - conPool.size());
            addConnectionsToPool(conToAdd);
        } else if (newSize < conPool.size()) //try to remove available connections
        {
            boolean done = false;
            while ((newSize < conPool.size()) && (!done)) {
                done = true;

                for (int i = 0; i < conPool.size(); i++) {
                    PooledConnection pc = (PooledConnection) conPool.get(i);
                    if (pc.isAvailable()) {
                        //found an available connection, so close and remove it
                        closeConnection(pc.getConnection());
                        conPool.remove(i);
                        done = false;
                        break;
                    }
                }
            }

        }

        return (conPool.size());
    }

    /**
     * Makes a connection available for reuse (in the connection pool).
     *
     * @param conn connection
     */
    public void releaseConnection(Connection conn) {
        for (int i = 0; i < conPool.size(); i++) {
            PooledConnection pc = (PooledConnection) conPool.get(i);
            if (pc.getConnection().equals(conn))
                pc.setAvailable(true);
        }
    }

    /**
     * Closes the given connection.
     *
     * @param conn connection to close.
     */
    void closeConnection(Connection conn) {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            System.err.println("Exception: unable to close connection");
            e.printStackTrace();
        }
    }

    /**
     * Pooled connection object.
     *
     * @author Jeff S Smith
     */
    private class PooledConnection {
        /**
         * Database connection.
         */
        private Connection con;

        /**
         * Is this connection available?
         */
        private boolean available;

        /**
         * Constructor for PooledConnection object.
         */
        PooledConnection(Connection con, boolean available) {
            this.con = con;
            this.available = available;
        }

        /**
         * Get the connection.
         */
        Connection getConnection() {
            return con;
        }

        /**
         * Is this connection available.
         */
        boolean isAvailable() {
            return available;
        }

        /**
         * Set this connection to available.
         */
        void setAvailable(boolean available) {
            this.available = available;
        }
    }
}