package spark.streaming.function;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import spark.streaming.util.Configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Split extends BaseFunction implements FlatMapFunction<Row, Row> {

    public Split(Configuration config) {
        super(config);
    }

    @Override
    public Iterator<Row> call(Row s) throws Exception {
        super.calculateThroughput();
        String[] words = s.getString(0).split("\\W");
        List<Row> tuples = new ArrayList<>();

        for (String word : words) {
            if (!StringUtils.isBlank(word))
                tuples.add(RowFactory.create(word, s.get(s.size() - 1)));
        }
        return tuples.iterator();
    }
}