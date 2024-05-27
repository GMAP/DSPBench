package flink.source;

import flink.constants.BaseConstants;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class InfSourceFunction extends RichParallelSourceFunction<String> {
    private volatile boolean isRunning = true;
    private String sourcePath;
    private long runTimeSec;

    public InfSourceFunction(Configuration config, String prefix) {
        this.sourcePath = config.getString(String.format(BaseConstants.BaseConf.SOURCE_PATH, prefix),"");
        this.runTimeSec = config.getInteger(String.format(BaseConstants.BaseConf.RUNTIME, prefix), 60);
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        try {

            long epoch = System.nanoTime();
            Scanner scanner = new Scanner(new File(sourcePath));

            while (isRunning && (System.nanoTime() - epoch < runTimeSec * 1e9)) {
                String line = scanner.nextLine();
                if(!scanner.hasNextLine()){
                    scanner = new Scanner(new File(sourcePath));
                }
                if (!StringUtils.isBlank(line))
                    ctx.collect(line);
            }

            scanner.close();

            isRunning = false;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
