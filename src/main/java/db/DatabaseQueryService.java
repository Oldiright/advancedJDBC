package db;

import db.dao.HighestPaidWorker;
import db.dao.LongestProject;
import db.dao.MaxProjectCountClient;
import db.dao.Type;
import db.dao.YoungestOrEldestWorker;
import util.Util;

import java.io.FileNotFoundException;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class DatabaseQueryService {
    private static final String LONGEST_PROJECT_QUERY = "sql/find_longest_project.sql";
    private static final String MAX_PROJECTS_CLIENT_QUERY = "sql/find_max_projects_client.sql";
    private static final String MAX_SALARY_WORKER_QUERY = "sql/find_max_salary_worker.sql";
    private static final String YOUNGEST_AND_ELDEST_WORKERS_QUERY = "sql/find_youngest_eldest_workers.sql";



    public List<LongestProject> findLongestProject() throws FileNotFoundException, SQLException {
        ResultSet rs = Database.getInstance().getConnection().createStatement()
                .executeQuery(Util.readInitFile(LONGEST_PROJECT_QUERY));
        ArrayList<LongestProject> result = new ArrayList<>();
        while (rs.next()) {
            LongestProject currentRow = new LongestProject();
            currentRow.setId(BigInteger.valueOf(rs.getInt("id")));
            currentRow.setDuration(rs.getInt("count_day"));
            result.add(currentRow);
        }

        return result;
    }

    public List<MaxProjectCountClient> findMaxProjectsClient() throws SQLException, FileNotFoundException {

        ResultSet rs = Database.getInstance().getConnection().createStatement()
                .executeQuery(Util.readInitFile(MAX_PROJECTS_CLIENT_QUERY));
        ArrayList<MaxProjectCountClient> result = new ArrayList<>();
        while (rs.next()) {
            MaxProjectCountClient currentRow = new MaxProjectCountClient();
            currentRow.setName(rs.getString("name"));
            currentRow.setProjectCount(rs.getInt("project_count"));
            result.add(currentRow);
        }

        return result;
    }

    public List<HighestPaidWorker> findMaxSalaryWorker() throws SQLException, FileNotFoundException {
        ResultSet rs = Database.getInstance().getConnection().createStatement()
                .executeQuery(Util.readInitFile(MAX_SALARY_WORKER_QUERY));
        ArrayList<HighestPaidWorker> result = new ArrayList<>();
        while (rs.next()) {
            HighestPaidWorker currentRow = new HighestPaidWorker();
            currentRow.setName(rs.getString("name"));
            currentRow.setSalary(rs.getInt("salary"));
            result.add(currentRow);
        }
        return result;
    }

    public List<YoungestOrEldestWorker> findYoungestAndEldestWorkers() throws SQLException, FileNotFoundException {
        ResultSet rs = Database.getInstance().getConnection().createStatement()
                .executeQuery(Util.readInitFile(YOUNGEST_AND_ELDEST_WORKERS_QUERY));
        ArrayList<YoungestOrEldestWorker> result = new ArrayList<>();
        while (rs.next()) {
            YoungestOrEldestWorker currentRow = new YoungestOrEldestWorker();
            currentRow.setName(rs.getString("name"));
            currentRow.setType(Type.valueOf(rs.getString("type")));
            currentRow.setBirthday(LocalDate.parse(rs.getString("birthday")));
            result.add(currentRow);
        }

        return result;
    }
}
