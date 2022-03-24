package spark.streaming.source;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import spark.streaming.constants.BaseConstants.BaseConfig;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

/**
 *
 * @author mayconbordin
 */
public class FileSource extends BaseSource {
    private int sourceThreads;
    private String sourcePath;
    
    @Override
    public void initialize(Configuration config, JavaStreamingContext context, String prefix) {
        super.initialize(config, context, prefix);
        
        sourceThreads = config.getInt(String.format(BaseConfig.SOURCE_THREADS, prefix), 1);
        sourcePath    = config.get(String.format(BaseConfig.SOURCE_PATH, prefix));
    }

    @Override
    public JavaDStream<Tuple2<String, Tuple>> createStream() {
        return context.textFileStream(sourcePath).map(s -> new Tuple2<>(s, new Tuple()));
    }
    
}
