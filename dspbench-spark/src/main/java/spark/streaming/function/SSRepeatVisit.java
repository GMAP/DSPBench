package spark.streaming.function;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.streaming.constants.MachineOutlierConstants;
import spark.streaming.util.Configuration;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

/**
 * @author luandopke
 */
public class SSRepeatVisit extends BaseFunction implements MapFunction<Row, Row> {
    private static final Logger LOG = LoggerFactory.getLogger(SSRepeatVisit.class);

    private Map<String, Void> map;

    public SSRepeatVisit(Configuration config) {
        super(config);
        map = new HashMap<>();
    }

    @Override
    public Row call(Row input) throws Exception {
        String clientKey = input.getString(2);
        String url =  input.getString(1);
        String key = url + ":" + clientKey;

        if (map.containsKey(key)) {
            return RowFactory.create(clientKey, url, Boolean.FALSE);
        } else {
            map.put(key, null);
            return RowFactory.create(clientKey, url, Boolean.TRUE);
        }
    }
}