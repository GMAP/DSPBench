package spark.streaming.model;

import java.io.Serializable;

public class VisitStats implements Serializable {

    private int total;
    private int uniqueCount;

    public VisitStats() {
        this.total = 0;
        this.uniqueCount = 0;
    }

    public int getTotal() {
        return total;
    }

    public int getUniqueCount() {
        return uniqueCount;
    }

    public void add(boolean unique) {
        total++;
        if (unique) uniqueCount++;
    }
}
