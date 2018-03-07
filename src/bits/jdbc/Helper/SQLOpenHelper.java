package bits.jdbc.Helper;

import bits.jdbc.database.Database;

public abstract class SQLOpenHelper {
    private final String mName;
    private String mUsername;
    private String mPassword;
    private Database mDatabase;
    private SQLConnectionPool mConnectionPool;

    public SQLOpenHelper(String name, String username, String password) {
        this.mName = name;
        this.mUsername = username;
        this.mPassword = password;
        mConnectionPool = new SQLConnectionPool(mName, username, password);
    }

    /**
     * Return the name of the SQL database being opened, as given to
     * the constructor.
     */
    public String getDatabaseName() {
        return mName;
    }

    public Database getWritableDatabase() {
        Database db = mDatabase;
        if (db == null) {
            if (mConnectionPool != null)
                mConnectionPool = new SQLConnectionPool(mName, mUsername, mPassword);
            db = new Database(mConnectionPool);
            onCreate(db);
        } else {
            onDowngrade(db);
        }

        db = mDatabase;

        return db;
    }

    /**
     * Close any open database object.
     */
    public void close() {
        if (mDatabase != null) {
            mDatabase.close();
            mDatabase = null;
        }
    }


    /**
     * Called when the database is created for the first time. This is where the
     * creation of tables and the initial population of the tables should happen.
     *
     * @param db The database.
     */
    public abstract void onCreate(Database db);

    public abstract void onDowngrade(Database db);

    public void onUpgrade(Database db) {

    }

}
