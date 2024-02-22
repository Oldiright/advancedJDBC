package db;

import util.Util;

import java.io.FileNotFoundException;


public class DatabaseInitService {
    private static final String INIT_FILE = "sql/init_db.sql";
    public static void main(String[] args)  {
        Database database = Database.getInstance();
        try {
            database.update(Util.readInitFile(INIT_FILE));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        database.closeConnection();
    }

}
