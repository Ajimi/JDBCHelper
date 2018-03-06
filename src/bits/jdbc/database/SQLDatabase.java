package bits.jdbc.database;

import bits.jdbc.Helper.ConnectionPool;
import bits.jdbc.content.ContentValues;
import bits.jdbc.utils.TextUtils;

import java.sql.*;

public class SQLDatabase {
    private ConnectionPool mConnectionPool;

    public SQLDatabase(ConnectionPool connPool) {
        this.mConnectionPool = connPool;
    }

    /**
     * Add the names that are not-null in columns to s, separating them with commas.
     */
    private static void appendColumns(StringBuilder s, String[] columns) {
        int n = columns.length;

        for (int i = 0; i < n; i++) {
            String column = columns[i];

            if (column != null) {
                if (i > 0) {
                    s.append(", ");
                }
                s.append(column);
            }
        }
        s.append(' ');
    }

    /**
     * Add the clause tha are not-null in clause to s.
     */
    private static void appendClause(StringBuilder s, String clause) {
        if (!TextUtils.isEmpty(clause)) {
            s.append(" WHERE ");
            s.append(clause);
        }
    }

    /**
     * Get JDBC connection pool.
     *
     * @return JDBC connection pool.
     */
    public ConnectionPool getConnectionPool() {
        return mConnectionPool;
    }

    /**
     * Close the database.
     */
    public void close() {
        mConnectionPool.closeAllConnections();
    }

    /**
     * Execute a single SQL statement that is NOT a SELECT or any other SQL statement that returns data.
     *
     * @param sql The SQL statement to be executed. Multiple statements separated by semicolons are
     *            not supported.
     * @throws SQLException
     */
    public void execSQL(String sql) throws SQLException {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = mConnectionPool.getConnection();
            statement = connection.createStatement();
            statement.execute(sql);
        } finally {
            if (statement != null) {
                statement.closeOnCompletion();
            }
            mConnectionPool.releaseConnection(connection);
        }
    }

    /**
     * Execute a single SQL statement that is NOT a SELECT or any other SQL statement that returns data.
     *
     * @param sql      The SQL statement to be executed. Multiple statements separated by semicolons are
     *                 not supported.
     * @param bindArgs You may include ?s in where clause in the query, which will be replaced by the
     *                 values from bindArgs.
     * @throws SQLException
     */
    public void execSQL(String sql, Object[] bindArgs) throws SQLException {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = mConnectionPool.getConnection();
            statement = connection.prepareStatement(sql);
            for (int i = 0; i < bindArgs.length; i++) {
                statement.setObject(i + 1, bindArgs[i]);
            }
            statement.execute();
        } finally {
            if (statement != null) {
                statement.closeOnCompletion();
            }
            mConnectionPool.releaseConnection(connection);
        }
    }

    /**
     * Execute a single SQL statement that is NOT a SELECT or any other SQL statement that returns data.
     *
     * @param sql      The SQL statement to be executed. Multiple statements separated by semicolons are
     *                 not supported.
     * @param bindArgs You may include ?s in where clause in the query, which will be replaced by the
     *                 values from bindArgs.
     * @return Either (1) the row count for SQL Data Manipulation Language (DML) statements or
     * (2) 0 for SQL statements that return nothing
     * @throws SQLException
     */
    public int executeUpdate(String sql, Object[] bindArgs) throws SQLException {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = mConnectionPool.getConnection();
            statement = connection.prepareStatement(sql);
            if (bindArgs != null) {
                for (int i = 0; i < bindArgs.length; i++) {
                    statement.setObject(i + 1, bindArgs[i]);
                }
            }
            return statement.executeUpdate();
        } finally {
            if (statement != null) {
                statement.closeOnCompletion();
            }
            mConnectionPool.releaseConnection(connection);
        }
    }

    /**
     * Runs the provided SQL and return a {@link ResultSet} over the result set.
     *
     * @param sql       The SQL query.
     * @param whereArgs You may include ?s in where clause in the query,
     *                  which will be replaced by the values from selectionArgs. The
     *                  values will be bound as Strings.
     * @return A {@link ResultSet} object, which is positioned before the first entry.
     * @throws SQLException
     */
    public ResultSet rawQuery(String sql, String[] whereArgs) throws SQLException {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = mConnectionPool.getConnection();
            statement = connection.prepareStatement(sql);
            if (whereArgs != null) {
                for (int i = 0; i < whereArgs.length; i++) {
                    statement.setString(i + 1, whereArgs[i]);
                }
            }
            return statement.executeQuery();
        } finally {
            if (statement != null) {
                statement.closeOnCompletion();
            }
            mConnectionPool.releaseConnection(connection);
        }
    }

    /**
     * Query the given table, returning a {@link ResultSet} over the result set.
     *
     * @param table       The table name to compile the query against.
     * @param columns     A list of which columns to return. Passing null will return all columns,
     *                    which is discouraged to prevent reading data from storage that isn't going
     *                    to be used.
     * @param whereClause A filter declaring which rows to return, formatted as an SQL WHERE clause
     *                    (excluding the WHERE ifself). Passing null will return all rows for the given table.
     * @param whereArgs   You may include ?s in selection, which will be replaced by the values from
     *                    selectionArgs, in order that they appear in the selection. The values will
     *                    be bound as Strings.
     * @return A {@link ResultSet} object, which is positioned before the first entry.
     */
    public ResultSet query(String table, String[] columns, String whereClause, String[] whereArgs)
            throws SQLException {
        StringBuilder query = new StringBuilder(120);

        query.append("SELECT ");
        if (columns != null && columns.length != 0) {
            appendColumns(query, columns);
        } else {
            query.append("* ");
        }
        query.append("FROM ")
                .append(table);
        appendClause(query, whereClause);

        return rawQuery(query.toString(), whereArgs);
    }

    /**
     * General method for inserting a row into the database.
     *
     * @param table         The table to insert the row into.
     * @param initialValues This map contains the initial columns values for the row. The keys should be
     *                      the column names and the values the column values.
     * @return Either (1) the row count for SQL Data Manipulation Language (DML) statements or (2)
     * 0 for SQL statements that return nothing.
     */
    public long insert(String table, String nullColumnHack, ContentValues initialValues) throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT ")
                .append("INTO ")
                .append(table)
                .append('(');

        Object[] bindArgs = null;
        int size = (initialValues != null && initialValues.size() > 0) ? initialValues.size() : 0;
        if (size > 0) {
            bindArgs = new Object[size];
            int i = 0;
            for (String colName : initialValues.keySet()) {
                sql.append((i > 0) ? "," : "");
                sql.append(colName);
                bindArgs[i++] = initialValues.get(colName);
            }
            sql.append(')');
            sql.append(" VALUES (");
            for (i = 0; i < size; i++) {
                sql.append((i > 0) ? ",?" : "?");
            }
        } else {
            sql.append(nullColumnHack).append(") VALUES (NULL");
        }

        sql.append(')');

        return executeUpdate(sql.toString(), bindArgs);
    }

    /**
     * Convenience method for updating rows in the database.
     *
     * @param table       The table to update in.
     * @param values      A map from column names to new column values. null is a valid value that will
     *                    be translated to NULL.
     * @param whereClause The optional WHERE clause to apply when updating. Passing null will
     *                    update all rows.
     * @param whereArgs   You may include ?s in the where clause, which
     *                    will be replaced by the values from whereArgs. The values
     *                    will be bound as Strings.
     * @return The number of rows affected.
     */
    public int update(String table, ContentValues values, String whereClause, String[] whereArgs)
            throws SQLException {

        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException("Empty values");
        }

        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE ")
                .append(table)
                .append(" SET ");
        // Move all bind args to one array.
        int setValuesSize = values.size();
        int bindArgsSize = (whereArgs == null) ? setValuesSize : (setValuesSize + whereArgs.length);
        Object[] bindArgs = new Object[bindArgsSize];
        int i = 0;
        for (String colName : values.keySet()) {
            sql.append((i > 0) ? "," : "")
                    .append(colName);
            bindArgs[i++] = values.get(colName);
            sql.append("=?");
        }

        if (whereArgs != null) {
            for (i = setValuesSize; i < bindArgsSize; i++) {
                bindArgs[i] = whereArgs[i - setValuesSize];
            }
        }
        if (!TextUtils.isEmpty(whereClause)) {
            sql.append(" WHERE ");
            sql.append(whereClause);
        }

        return executeUpdate(sql.toString(), bindArgs);
    }

    /**
     * Convenience method for deleting rows in the database.
     *
     * @param table       The table to delete from.
     * @param whereClause The optional WHERE clause to apply when deleting. Passing null will delete
     *                    all rows.
     * @param whereArgs   You may include ?s in the where clause, which
     *                    will be replaced by the values from whereArgs. The values
     *                    will be bound as Strings.
     * @return The number of rows affected if a whereClause is passed in, 0 otherwise. To remove all
     * rows and get a count pass "1" as the whereClause.
     */
    public int delete(String table, String whereClause, String[] whereArgs) throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM ")
                .append(table);
        if (!TextUtils.isEmpty(whereClause)) {
            sql.append(" WHERE ")
                    .append(whereClause);
        }

        return executeUpdate(sql.toString(), whereArgs);
    }
}