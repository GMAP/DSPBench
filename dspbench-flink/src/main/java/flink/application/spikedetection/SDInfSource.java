package flink.application.spikedetection;

import flink.constants.BaseConstants;
import flink.constants.SpikeDetectionConstants;
import flink.util.Configurations;
import flink.util.Metrics;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

import com.google.common.collect.ImmutableMap;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Date;
import java.util.Scanner;

public class SDInfSource extends RichParallelSourceFunction<Tuple3<String, Date, Double>> {
    private volatile boolean isRunning = true;
    private String sourcePath;
    private long runTimeSec;
    Configuration config;

    private final String valueField;
    private final int valueFieldKey;

    Metrics metrics = new Metrics();

    private static final ImmutableMap<String, Integer> fieldList = ImmutableMap.<String, Integer>builder()
            .put("temp", 4)
            .put("humid", 5)
            .put("light", 6)
            .put("volt", 7)
            .build();

    public SDInfSource(Configuration config, String prefix) {
        metrics.initialize(config, this.getClass().getSimpleName());
        this.config = config;
        this.sourcePath = config.getString(String.format(BaseConstants.BaseConf.SOURCE_PATH, prefix),"");
        this.runTimeSec = config.getInteger(String.format(BaseConstants.BaseConf.RUNTIME, prefix), 60);
        valueField = config.getString(SpikeDetectionConstants.Conf.PARSER_VALUE_FIELD, "temp");
        valueFieldKey = fieldList.get(valueField);
    }

    @Override
    public void cancel() {
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.SaveMetrics();
        }
        isRunning = false;
    }

    public void close() throws Exception {
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.SaveMetrics();
        }
    }

    @Override
    public void run(SourceContext<Tuple3<String, Date, Double>> ctx) throws Exception {
        DateTimeFormatter formatterMillis = new DateTimeFormatterBuilder()
        .appendYear(4, 4).appendLiteral("-").appendMonthOfYear(2).appendLiteral("-")
        .appendDayOfMonth(2).appendLiteral(" ").appendHourOfDay(2).appendLiteral(":")
        .appendMinuteOfHour(2).appendLiteral(":").appendSecondOfMinute(2)
        .appendLiteral(".").appendFractionOfSecond(3, 6).toFormatter();

        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        try {

            long epoch = System.nanoTime();
            Scanner scanner = new Scanner(new File(sourcePath));

            while (isRunning && (System.nanoTime() - epoch < runTimeSec * 1e9)) {
                String line = scanner.nextLine();
                if(!scanner.hasNextLine()){
                    scanner = new Scanner(new File(sourcePath));
                }

                String[] temp = line.split("\\s+");

                if (temp.length == 8){

                    String dateStr = String.format("%s %s", temp[0], temp[1]);
                    DateTime date = null;

                    try {
                        date = formatterMillis.parseDateTime(dateStr);
                    } catch (IllegalArgumentException ex) {
                        try {
                            date = formatter.parseDateTime(dateStr);
                        } catch (IllegalArgumentException ex2) {
                            System.out.println("Error parsing record date/time field, input record: " + line + ", " + ex2);
                        }
                    }

                    try {
                        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
                            metrics.emittedThroughput();
                        }

                        ctx.collect(new Tuple3<>(
                            temp[3],
                            date.toDate(),
                            Double.parseDouble(temp[valueFieldKey])));
                    } catch (NumberFormatException ex) {
                        System.out.println("Error parsing record numeric field, input record: " + line + ", " + ex);
                    }
                }
            }

            scanner.close();

            isRunning = false;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
