package spark.streaming.application;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.streaming.constants.TrafficMonitoringConstants;
import spark.streaming.constants.WordCountConstants;
import spark.streaming.util.Configuration;

public class TrafficMonitoring extends AbstractApplication {
    private static final Logger LOG = LoggerFactory.getLogger(TrafficMonitoring.class);
    private String checkpointPath;
    private int batchSize;
    private int parserThreads;
    private int mapMatcherThreads;
    private int speedCalculatorThreads;
    public TrafficMonitoring(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        checkpointPath         = config.get(getConfigKey(TrafficMonitoringConstants.Config.CHECKPOINT_PATH), ".");
        batchSize              = config.getInt(getConfigKey(TrafficMonitoringConstants.Config.BATCH_SIZE), 1000);
        parserThreads          = config.getInt(TrafficMonitoringConstants.Config.PARSER_THREADS, 1);
        mapMatcherThreads      = config.getInt(TrafficMonitoringConstants.Config.MAP_MATCHER_THREADS, 1);
        speedCalculatorThreads = config.getInt(TrafficMonitoringConstants.Config.SPEED_CALCULATOR_THREADS, 1);
    }

    @Override
    public DataStreamWriter buildApplication() {

        Dataset<Row> rawRecords = createSource();
        //USAR ENCODE E NAO CRIAR UM DATASET NOVO UNICAMENTE PARA FAZER O PARSER, POIS IREMOS FAZER OS TESTES COM O KAFKA Q ÉFODACE PRA ISSO



        return null;
    }

    @Override
    public String getConfigPrefix() {
        return TrafficMonitoringConstants.PREFIX;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }
}
