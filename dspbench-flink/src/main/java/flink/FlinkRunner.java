package flink;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.collect.Lists;

import flink.application.YSB.YSB;
import flink.application.adanalytics.AdAnalytics;
import flink.application.bargainindex.BargainIndex;
import flink.application.clickanalytics.ClickAnalytics;
import flink.application.frauddetection.FraudDetection;
import flink.application.logprocessing.LogProcessing;
import flink.application.machineoutiler.MachineOutlier;
import flink.application.reinforcementlearner.ReinforcementLearner;
import flink.application.sentimentanalysis.SentimentAnalysis;
import flink.application.smartgrid.SmartGrid;
import flink.application.spamfilter.SpamFilter;
import flink.application.spikedetection.SpikeDetection;
import flink.application.trafficmonitoring.TrafficMonitoring;
import flink.application.trendingtopics.TrendingTopics;
import flink.application.voipstream.VoIPStream;
import flink.application.wordcount.WordCount;
import flink.util.Configurations;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * Utility class to run a Spark Streaming application
 *
 * @author Gabriel Rustick Fim
 */
public class FlinkRunner {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkRunner.class);
    private static final String CFG_PATH = "/config/%s.properties";

    @Parameter
    public List<String> parameters = Lists.newArrayList();

    @Parameter(names = {"-a", "--app"}, description = "The application to be executed", required = true)
    public String application;

    @Parameter(names = {"-n", "--application-name"}, required = false, description = "The name of the topology")
    public String applicationName;

    @Parameter(names = {"--config"}, required = false, description = "Path to the configuration file for the application")
    public String configStr;

    private final AppDriver driver;
    private Configuration config;

    public FlinkRunner() {
        driver = new AppDriver();

        driver.addApp("clickanalytics", ClickAnalytics.class);
        driver.addApp("frauddetection", FraudDetection.class);
        driver.addApp("logprocessing", LogProcessing.class);
        driver.addApp("machineoutlier", MachineOutlier.class);
        driver.addApp("sentimentanalysis", SentimentAnalysis.class);
        driver.addApp("spikedetection", SpikeDetection.class);
        driver.addApp("smartgrid", SmartGrid.class);
        driver.addApp("trafficmonitoring", TrafficMonitoring.class);
        driver.addApp("wordcount", WordCount.class);
        driver.addApp("adanalytics", AdAnalytics.class);
        driver.addApp("bargainindex", BargainIndex.class);
        driver.addApp("reinforcementlearner", ReinforcementLearner.class);
        driver.addApp("spamfilter", SpamFilter.class);
        driver.addApp("trendingtopics", TrendingTopics.class);
        driver.addApp("voipstream", VoIPStream.class);
        
        driver.addApp("YSB", YSB.class);
    }

    public void run() throws InterruptedException {
        // Loads the configuration file set by the user or the default configuration
        try {
            // load default configuration
            if (configStr == null) {
                String cfg = String.format(CFG_PATH, application);
                Properties p = loadProperties(cfg, true);

                config = Configurations.fromProperties(p);
                LOG.info("Loaded default configuration file {}", cfg);
            } else {
                config = Configurations.fromStr(configStr);
                LOG.info("Loaded configuration from command line argument");
            }
        } catch (IOException ex) {
            LOG.error("Unable to load configuration file", ex);
            throw new RuntimeException("Unable to load configuration file", ex);
        }

        // Get the descriptor for the given application
        AppDriver.AppDescriptor app = driver.getApp(application);
        if (app == null) {
            throw new RuntimeException("The given application name " + application + " is invalid");
        }

        // In case no topology names is given, create one
        if (applicationName == null) {
            applicationName = String.format("%s-%d", application, new Random().nextInt());
        }

        StreamExecutionEnvironment exec = app.getContext(applicationName, config);

        try {
            exec.execute(applicationName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public static void main(String[] args) throws Exception {
        FlinkRunner runner = new FlinkRunner();
        JCommander cmd = new JCommander(runner);

        try {
            cmd.parse(args);
        } catch (ParameterException ex) {
            System.err.println("Argument error: " + ex.getMessage());
            cmd.usage();
            System.exit(1);
        }

        runner.run();
    }

    public static Properties loadProperties(String filename, boolean classpath) throws IOException {
        Properties properties = new Properties();
        InputStream is;

        if (classpath) {
            is = FlinkRunner.class.getResourceAsStream(filename);
        } else {
            is = new FileInputStream(filename);
        }

        properties.load(is);
        is.close();

        return properties;
    }
}
