package com.streamer.topology.impl;

import com.codahale.metrics.MetricRegistry;
import com.streamer.topology.TaskRunner;
import com.streamer.metrics.MetricsFactory;

/**
 *
 * @author mayconbordin
 */
public class LocalTaskRunner extends TaskRunner {
    private static MetricRegistry metrics;
    
    public LocalTaskRunner(String[] args) {
        super(args);
    }
    
    public static void main(String[] args) {
        LocalTaskRunner taskRunner = new LocalTaskRunner(args);
        LocalComponentFactory factory = new LocalComponentFactory();
        factory.setConfiguration(taskRunner.getConfiguration());
        
        metrics = MetricsFactory.createRegistry(taskRunner.getConfiguration());
        factory.setMetrics(metrics);
        
        taskRunner.start(factory);
        
        LocalEngine engine = LocalEngine.getEngine();
        engine.submitTopology(taskRunner.getTopology());
        engine.run();
    }
}
