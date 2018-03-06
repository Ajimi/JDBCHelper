package bits.jdbc.Helper;

import java.sql.Connection;
import java.sql.SQLException;


/**
 * Constants define the different database types
 *
 * @author Jeff S Smith
 * @author Paolo Orru (paolo.orru@gmail.com), added PostgreSQL constants
 */
public class DatabaseType {
    public final static int UNKNOWN = 0;
    public final static int ORACLE = 1;
    public final static int MYSQL = 2;
    public final static int POSTGRESQL = 3;
    public final static int HSQL = 4;

    public final static String ORACLE_NAME = "ORACLE";
    public final static String MYSQL_NAME = "MYSQL";
    public final static String POSTGRESQL_NAME = "POSTGRESQL";
    public final static String HSQL_NAME = "HSQL";

    /**
     * Parses the connection info to determine the database type
     *
     * @param con Connection
     * @return int type of database (e.g. ORACLE)
     */
    static int getDbType(Connection con) {
        String dbName = null;
        int dbType = 0;

        try {
            dbName = con.getMetaData().getDatabaseProductName();

            if (dbName.toUpperCase().contains(ORACLE_NAME))
                dbType = ORACLE;
            else if (dbName.toUpperCase().contains(MYSQL_NAME))
                dbType = MYSQL;
            else if (dbName.toUpperCase().contains(POSTGRESQL_NAME))
                dbType = POSTGRESQL;
            else if (dbName.toUpperCase().contains(HSQL_NAME))
                dbType = HSQL;
        } catch (SQLException e) {
            System.out.println("Exception: unknown database");
            e.printStackTrace();
        }
        return (dbType);
    }

    /**
     * Parses the driver name to determine the database type
     *
     * @param driverName String
     * @return int type of database (e.g. ORACLE)
     */
    static int getDbType(String driverName) {
        int dbType = 0;
        if (driverName.toUpperCase().contains(ORACLE_NAME))
            dbType = ORACLE;
        else if (driverName.toUpperCase().contains(MYSQL_NAME))
            dbType = MYSQL;
        else if (driverName.toUpperCase().contains(POSTGRESQL_NAME))
            dbType = POSTGRESQL;
        else if (driverName.toUpperCase().contains(HSQL_NAME))
            dbType = HSQL;
        return (dbType);
    }
}