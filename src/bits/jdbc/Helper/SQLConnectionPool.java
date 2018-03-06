package bits.jdbc.Helper;

public class SQLConnectionPool extends ConnectionPool {
    private static final String DRIVER_NAME = "com.mysql.jdbc.Driver";
    private static final String CONNECTION_URL_BASE = "jdbc:mysql://localhost:3306/%s";
    private static final int DEFAULT_CONNECTION_NUM = 5;

    //jdbc:mysql://localhost:3306/russia

    public SQLConnectionPool(String name) {
        super(DEFAULT_CONNECTION_NUM, DRIVER_NAME, String.format(CONNECTION_URL_BASE, name), "", "");
    }
}
