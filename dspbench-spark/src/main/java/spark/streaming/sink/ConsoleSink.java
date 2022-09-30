package spark.streaming.sink;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;

public class ConsoleSink extends BaseSink {

    @Override
    public DataStreamWriter<Row> sinkStream(Dataset<Row> dt) {
        return dt.writeStream().foreach(new ForeachWriter<Row>() {

            @Override
            public boolean open(long partitionId, long version) {
                return true;
            }

            @Override
            public void process(Row value) {
                System.out.println(value);
            }

            @Override
            public void close(Throwable errorOrNull) {
                // Close the connection
            }
        }).outputMode("update");
    }
}
