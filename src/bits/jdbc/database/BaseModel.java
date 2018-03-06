package bits.jdbc.database;

import bits.jdbc.content.ContentValues;

import java.sql.ResultSet;

public abstract class BaseModel {
    public BaseModel() {
    }

    public BaseModel(ResultSet resultSet) {
        resolve(resultSet);
    }

    public abstract ContentValues assemble();

    public abstract void resolve(ResultSet resultSet);
}