package spark.streaming.source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

/**
 *
 * @author mayconbordin
 */
public abstract class BaseSource {
    protected Configuration config;
    protected transient SparkSession session;
    
    public void initialize(Configuration config, SparkSession session, String prefix) {
        this.config = config;
        this.session = session;
    }
    
    public abstract Dataset<Row> createStream();
}
