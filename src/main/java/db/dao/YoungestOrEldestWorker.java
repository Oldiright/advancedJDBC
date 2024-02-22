package db.dao;

import java.time.LocalDate;

public class YoungestOrEldestWorker {
    private String name;
    private Type type;
    private LocalDate birthday;

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public LocalDate getBirthday() {
        return birthday;
    }

    public void setBirthday(LocalDate birthday) {
        this.birthday = birthday;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "YoungestOrEldestWorker{" +
                "name='" + name + '\'' +
                ", type=" + type +
                ", birthday=" + birthday +
                '}';
    }
}
