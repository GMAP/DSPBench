package flink.application.YSB;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.streaming.api.windowing.time.Time;
import flink.application.AbstractApplication;
import flink.constants.YSBConstants;
import flink.source.CampaignAd;
import flink.source.YSBSource;
import flink.source.YSB_Event;
import flink.sink.ConsoleSink;
import flink.sink.DroppedSink;

public class YSB extends AbstractApplication{
    private static final Logger LOG = LoggerFactory.getLogger(YSB.class);

    private long runTimeSec;
    private int numCampaigns;
    private int sourceThreads;
    private int filterThreads;
    private int joinerThreads;
    private int aggregatorThreads;

    public YSB(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        sourceThreads = config.getInteger(YSBConstants.Conf.SOURCE_THREADS, 1);
        filterThreads = config.getInteger(YSBConstants.Conf.FILTER_THREADS, 1);
        joinerThreads = config.getInteger(YSBConstants.Conf.JOINER_THREADS, 1);
        aggregatorThreads = config.getInteger(YSBConstants.Conf.AGGREGATOR_THREADS, 1);

        numCampaigns = config.getInteger(String.format(YSBConstants.Conf.RUNTIME, getConfigPrefix()), 60);
        runTimeSec = config.getInteger(String.format(YSBConstants.Conf.NUM_CAMPAIGNS, getConfigPrefix()), 60);
    }

    @Override
    public StreamExecutionEnvironment buildApplication() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<CampaignAd> campaignAdSeq = generateCampaignMapping(numCampaigns);
        HashMap<String, String> campaignLookup = new HashMap<String, String>();
        for (int i=0; i<campaignAdSeq.size(); i++) {
            campaignLookup.put((campaignAdSeq.get(i)).ad_id, (campaignAdSeq.get(i)).campaign_id);
        }

        final OutputTag<Joined_Event> lateOutputTag = new OutputTag<Joined_Event>("late-data"){};

        // Spout
        YSBSource source = new YSBSource(config, campaignAdSeq, numCampaigns, runTimeSec);
        DataStream<YSB_Event> data = env.addSource(source).setParallelism(sourceThreads);
        

        // Filter
        DataStream<YSB_Event> filtered = data.filter(new Filter(config)).setParallelism(filterThreads);
        
        // Joiner
        DataStream<Joined_Event> joiner = filtered.flatMap(new Joiner(config, campaignLookup)).setParallelism(joinerThreads);

        // Aggregator
        SingleOutputStreamOperator<Aggregate_Event> aggregator = joiner.keyBy("cmp_id")
        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
        .sideOutputLateData(lateOutputTag)
        .aggregate(new Aggregator(config))
        .setParallelism(aggregatorThreads);

        // Late
        DataStream<Joined_Event> lateStream = aggregator.getSideOutput(lateOutputTag);

        // Sinks
        lateStream.addSink(new DroppedSink()).setParallelism(1);
        createSinkYSB(aggregator);

        return env;
    }

    @Override
    public String getConfigPrefix() {
        return YSBConstants.PREFIX;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }
    
    private static List<CampaignAd> generateCampaignMapping(int numCampaigns) {
        CampaignAd[] campaignArray = new CampaignAd[numCampaigns*10];
        for (int i=0; i<numCampaigns; i++) {
            String campaign = UUID.randomUUID().toString();
            for (int j=0; j<10; j++) {
                campaignArray[(10*i)+j] = new CampaignAd(UUID.randomUUID().toString(), campaign);
            }
        }
        return Arrays.asList(campaignArray);
    }
}
