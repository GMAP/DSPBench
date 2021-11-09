package spark.streaming.source;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import spark.streaming.constants.BaseConstants.BaseConfig;
import spark.streaming.receiver.FileReceiver;
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
        List<JavaDStream<Tuple2<String, Tuple>>> streams = new ArrayList<>(sourceThreads);
        for (int i = 0; i < sourceThreads; i++) {
            streams.add(context.receiverStream(new FileReceiver(sourcePath)));
        }
        
        JavaDStream<Tuple2<String, Tuple>> stream;
        
        if (streams.size() > 1) {
            stream = context.union(streams.get(0), streams.subList(1, streams.size()));
        } else {
            stream = streams.get(0);
        }
        
        return stream;
    }
    
}
