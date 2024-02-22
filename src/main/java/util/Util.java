package util;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class Util {
    public static String readInitFile(String fileName) throws FileNotFoundException {
        File file = new File(fileName);
        Scanner scanner = new Scanner(file);
        StringBuilder sql = new StringBuilder();
        while(scanner.hasNext()) {
            sql.append(scanner.nextLine());
            sql.append("\n");
        }
        scanner.close();
        return sql.toString();
    }
}
