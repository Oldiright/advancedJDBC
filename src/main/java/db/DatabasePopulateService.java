package db;

import util.Util;

import java.io.FileNotFoundException;

public class DatabasePopulateService {
    private static final String POPULATE_FILE = "sql/populate_db.sql";

    public static void main(String[] args) {
        Database database = Database.getInstance();
        try {
            database.update(Util.readInitFile(POPULATE_FILE));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        database.closeConnection();
    }
}
