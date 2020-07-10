package com.streamer.topology.impl;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import com.streamer.core.Tuple;
import com.streamer.topology.TaskRunner;
import static com.streamer.topology.impl.StormConstants.CONF_LOCAL_MODE;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mayconbordin
 */
public class StormTaskRunner extends TaskRunner {
    private static final Logger logger = LoggerFactory.getLogger(StormTaskRunner.class);
    
    public StormTaskRunner(String[] args) {
        super(args);
    }

    public static void main(String[] args) {
        System.out.println(Arrays.toString(args));
        StormTaskRunner taskRunner = new StormTaskRunner(args);

        TopologyBuilder builder = new TopologyBuilder();
        StormComponentFactory factory = new StormComponentFactory(builder);
        factory.setConfiguration(taskRunner.getConfiguration());
        
        taskRunner.start(factory);
        
        Config config = new Config();
        config.putAll(taskRunner.getConfiguration());
        config.setDebug(taskRunner.isDebug());
        
        // register tuple serializer
        config.registerSerialization(Tuple.class, Tuple.TupleSerializer.class);
        
        boolean isLocal = taskRunner.getConfiguration().getBoolean(CONF_LOCAL_MODE, false);
            
        if (isLocal) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(taskRunner.getTopologyName() , config, builder.createTopology());

            backtype.storm.utils.Utils.sleep(600*1000);

            cluster.killTopology(taskRunner.getTopologyName());
            cluster.shutdown();
        } else {
            try {
                StormSubmitter.submitTopology(taskRunner.getTopologyName(), config, builder.createTopology());
            } catch (InvalidTopologyException ex) {
                logger.error("Invalid Topology", ex);
            } catch (AlreadyAliveException ex) {
                logger.error("Topology already exists", ex);
            }
        }
    }
}
