package db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;


public class Database {
    private static final Database INSTANCE = new Database();
    private  static final String dbUrl = "jdbc:h2:./test";
    private Connection connection;

    private Database() {
        try {
            connection = DriverManager.getConnection(dbUrl);
        } catch (Exception  ex) {
            ex.printStackTrace();
        }
    }
    public Connection getConnection() {
        return connection;
    }
    public static Database getInstance() {
        return INSTANCE;
    }
    public void closeConnection() {
        try {
            connection.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    public int update(String sql) {
        try(Statement statement = connection.createStatement()) {
            return statement.executeUpdate(sql);
        }  catch (Exception ex) {
            ex.printStackTrace();
        }
        return -1;
    }


}
