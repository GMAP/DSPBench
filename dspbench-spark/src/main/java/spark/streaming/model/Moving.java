package spark.streaming.model;

import java.io.Serializable;
import java.util.LinkedList;

public class Moving implements Serializable {
    private int id;
    private LinkedList<Double> list;
    private double sum = 0;

    public Moving(int id) {
        this.id = id;
        this.list = new LinkedList<>();
    }

    public LinkedList<Double> getList() {
        return list;
    }

    public void remove() {
        double valueToRemove = this.list.removeFirst();
        this.sum -= valueToRemove;
    }

    public void add(double value) {
        this.list.addLast(value);
        this.sum += value;
    }


    public double getSum() {
        return sum;
    }
}
