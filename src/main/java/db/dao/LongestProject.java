package db.dao;

import java.math.BigInteger;


public class LongestProject {
    private BigInteger id;
    private int duration;


    public BigInteger getId() {
        return id;
    }

    public void setId(BigInteger id) {
        this.id = id;
    }

    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }

    @Override
    public String toString() {
        return "LongestProject{" +
                "id=" + id +
                ", duration=" + duration +
                '}';
    }
}

