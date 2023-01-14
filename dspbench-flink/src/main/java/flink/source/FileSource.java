package flink.source;

import flink.constants.BaseConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FileSource extends BaseSource {
    private String sourcePath;

    @Override
    public void initialize (Configuration config, StreamExecutionEnvironment env, String prefix) {
        super.initialize(config, env, prefix);
        sourcePath = config.getString(String.format(BaseConstants.BaseConf.SOURCE_PATH, prefix),"");
    }

    @Override
    public DataStream<String> createStream() {
        return env.readTextFile(sourcePath);
    }
}
