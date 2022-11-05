package spark.streaming.function;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.streaming.util.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * @author luandopke
 */
public class SSVisitStats extends BaseFunction implements MapFunction<Row, Row> {
    private static final Logger LOG = LoggerFactory.getLogger(SSVisitStats.class);
    private int total = 0;
    private int uniqueCount = 0;

    public SSVisitStats(Configuration config) {
        super(config);
    }

    @Override
    public Row call(Row input) throws Exception {
        boolean unique = input.getBoolean(2);
        total++;
        if (unique) uniqueCount++;
        return RowFactory.create(total, uniqueCount);
    }
}