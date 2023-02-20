package flink.source;

import flink.constants.BaseConstants;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FileSourceInf extends BaseSourceInf implements MapFunction<String, String>{
    private String sourcePath;

    @Override
    public void initialize (Configuration config, StreamExecutionEnvironment env, String prefix) {
        super.initialize(config, env, prefix);
        sourcePath = config.getString(String.format(BaseConstants.BaseConf.SOURCE_PATH, prefix),"");
    }

    @Override
    public String createStream() throws Exception {
        return map(sourcePath);
    }

    @Override
    public String map(String value) throws Exception {
        try {
			Scanner scanner = new Scanner(new File(value));

			while (scanner.hasNextLine()) {
                String line = new String(scanner.nextLine());
                if(!scanner.hasNextLine()){
                    scanner = new Scanner(new File(value));
                }
				return line;
			}

			scanner.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
        return null;
    }
}
