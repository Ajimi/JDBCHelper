package bits.jdbc.Helper;

public class SQLConnectionPool extends ConnectionPool {
    private static final String DRIVER_NAME = "com.mysql.jdbc.Driver";
    private static final String CONNECTION_URL_BASE = "jdbc:mysql://localhost:3306/%s";
    private static final int DEFAULT_CONNECTION_NUM = 5;

    public SQLConnectionPool(String name) {

        this(name, "root", "");
    }

    public SQLConnectionPool(String name, String username) {
        this(name, username, "");
    }

    public SQLConnectionPool(String name, String username, String password) {
        super(DEFAULT_CONNECTION_NUM, DRIVER_NAME, String.format(CONNECTION_URL_BASE, name), username, password);
    }
}