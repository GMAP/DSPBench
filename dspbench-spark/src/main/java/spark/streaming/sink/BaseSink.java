package spark.streaming.sink;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import spark.streaming.util.Configuration;

import java.io.Serializable;

public abstract class BaseSink implements Serializable {
    protected Configuration config;
    protected transient SparkSession session;

    public void initialize(Configuration config, SparkSession session) {
        this.config = config;
        this.session = session;
    }

    public abstract DataStreamWriter<Row> sinkStream(Dataset<Row> dt);

}
