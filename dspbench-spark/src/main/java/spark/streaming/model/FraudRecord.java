package spark.streaming.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class FraudRecord implements Serializable {
    private List<String> list;

    public FraudRecord(String value) {
        this.list = new ArrayList<>();
        this.list.add(value);
    }

    public List<String> getList() {
        return list;
    }
    public void setList(List<String> l) {
        list = l;
    }
    public void add(String value) {
        list.add(value);
    }
}
