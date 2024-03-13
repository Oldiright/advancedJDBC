package db;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import db.dto.Project;
import db.dto.Worker;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class DatabasePopulateService {
    private static final String WORKERS_FILE = "sql/workers.json";
    private static final String PROJECTS_FILE = "sql/projects.json";
    private static final String PROJECT_WORKER = "sql/project_worker.json";
    private static final String SQL_FOR_WORKER_STATEMENT = "INSERT INTO worker(name, birthday, level, salary) VALUES (?, ?, ?, ?)";
    private static final String SQL_FOR_PROJECT_WORKER_STATEMENT = "INSERT INTO project_worker(project_id, worker_id) VALUES (?, ?)";
    private static final String SQL_FOR_CLIENTS = "INSERT INTO client(name) VALUES (?)";
    private static final String SQL_FOR_PROJECTS = "INSERT INTO project(client_id, start_date, finish_date) VALUES (?, ?, ?)";
    private static final List<String> CLIENTS = Arrays.asList("Empire", "Galactic Republic", "Tatooine", "Mandalor",
            "Sith",
            "Djedai");

    public static void main(String[] args) throws SQLException {
        Database database = Database.getInstance();

        //add workers
        PreparedStatement preparedStatement = getPreparedStatement(database, SQL_FOR_WORKER_STATEMENT);
        List<Worker> workers = getWorkerList();
        for(Worker worker: workers) {
            createNewWorker(worker, preparedStatement);
            preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();

        //add clients
        preparedStatement = getPreparedStatement(database, SQL_FOR_CLIENTS);
        for(String client: CLIENTS) {
            createClient(client, preparedStatement);
                preparedStatement.addBatch();
            }
        preparedStatement.executeBatch();


        //add projects
        preparedStatement = getPreparedStatement(database, SQL_FOR_PROJECTS);
        List<Project> projects = getProjectList();
        for(Project project: projects) {
            createNewProject(project, preparedStatement);
                preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();

        //add project workers
        preparedStatement = getPreparedStatement(database, SQL_FOR_PROJECT_WORKER_STATEMENT);

        List<List<String>> pw = getProjectWorkerList();


        for(List<String> projectWorker: pw) {
            createNewProjectWorker(projectWorker, preparedStatement);
            preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();

    }

    private static PreparedStatement getPreparedStatement(Database db, String sql) {
            PreparedStatement ps;
        try {
            ps = db.getConnection().prepareStatement(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return ps;
    }

    private static List<Worker> getWorkerList() {
         List<Worker> workers;
         Gson gson = new GsonBuilder().setPrettyPrinting().create();
        try {
            workers = gson.fromJson(new FileReader(WORKERS_FILE), TypeToken.getParameterized(List.class, Worker.class).getType());
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        return workers;
    }
    private static List<Project> getProjectList() {
        List<Project> projects;
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        try {
            projects = gson.fromJson(new FileReader(PROJECTS_FILE), TypeToken.getParameterized(List.class, Project.class).getType());
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        return projects;
    }
    private  static List<List<String>> getProjectWorkerList() {
        List<List<String>> pw;
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        try {
            pw = gson.fromJson(new FileReader(PROJECT_WORKER), TypeToken.getParameterized(List.class, List.class).getType());
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        return pw;
    }
    private static boolean createNewWorker(Worker worker, PreparedStatement preparedStatement) {
        try {
            preparedStatement.setString(1, worker.getName());
            preparedStatement.setString(2, worker.getBirthday());
            preparedStatement.setString(3, worker.getLevel());
            preparedStatement.setInt(4, worker.getSalary());

        } catch (Exception ex) {
            return false;
        }
        return true;
    }
    private static boolean createNewProject(Project project, PreparedStatement ps) {
        try {
            ps.setInt(1, project.getClient_id());
            ps.setString(2,project.getStart_date());
            ps.setString(3, project.getFinish_date());
        } catch (Exception ex) {
            return false;
        }
        return true;
    }
    private static void createNewProjectWorker(List<String> pw, PreparedStatement ps) {
        try {
            ps.setInt(1, Integer.parseInt(pw.get(0)));
            ps.setInt(2, Integer.parseInt(pw.get(1)));
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }
    private static boolean createClient(String client, PreparedStatement ps) {
        try {
            ps.setString(1, client);
        } catch (SQLException e) {
            return false;
        }
        return true;
    }
}
