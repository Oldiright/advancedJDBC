package db.dao;

public class HighestPaidWorker {
    String name;
    int salary;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getSalary() {
        return salary;
    }

    public void setSalary(int salary) {
        this.salary = salary;
    }

    @Override
    public String toString() {
        return "HighestPaidWorker{" +
                "name='" + name + '\'' +
                ", salary=" + salary +
                '}';
    }
}
