package flink.source;

import flink.constants.BaseConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class InfSourceFunction extends RichSourceFunction<String> {
    private volatile boolean isRunning = true;
    private String sourcePath;

    public InfSourceFunction(Configuration config, String prefix) {
        this.sourcePath = config.getString(String.format(BaseConstants.BaseConf.SOURCE_PATH, prefix),"");
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        try {
            Scanner scanner = new Scanner(new File(sourcePath));

            while (isRunning && scanner.hasNextLine()) {
                String line = scanner.nextLine();
                if(!scanner.hasNextLine()){
                    scanner = new Scanner(new File(sourcePath));
                }
                /*
                if (StringUtils.isBlank(line))
                    ctx.collect(null);

                ctx.collect(line);
                 */
                if (!StringUtils.isBlank(line))
                    System.out.println(line);
                    ctx.collect(line);
            }

            scanner.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
