package db.dto;

public class Project {

    private int client_id;
    private String start_date;
    private String finish_date;

    public int getClient_id() {
        return client_id;
    }

    public String getStart_date() {
        return start_date;
    }

    @Override
    public String toString() {
        return "Project{" +
                "client_id=" + client_id +
                ", start_date='" + start_date + '\'' +
                ", finish_date='" + finish_date + '\'' +
                '}';
    }

    public String getFinish_date() {
        return finish_date;
    }
}
