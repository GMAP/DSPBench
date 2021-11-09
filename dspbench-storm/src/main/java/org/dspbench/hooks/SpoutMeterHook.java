package org.dspbench.hooks;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.storm.hooks.BaseTaskHook;
import org.apache.storm.hooks.info.EmitInfo;
import org.apache.storm.hooks.info.SpoutAckInfo;
import org.apache.storm.task.TopologyContext;
import org.dspbench.metrics.MetricsFactory;
import org.dspbench.util.config.Configuration;

/**
 *
 * @author mayconbordin
 */
public class SpoutMeterHook extends BaseTaskHook {
    private Configuration config;
    private Meter emittedTuples;
    private Timer completeLatency;
    
    @Override
    public void prepare(Map conf, TopologyContext context) {
        config = Configuration.fromMap(conf);
        
        MetricRegistry registry = MetricsFactory.createRegistry(config);
        
        String componentId = context.getThisComponentId();
        String taskId      = String.valueOf(context.getThisTaskId());
        
        emittedTuples   = registry.meter(MetricRegistry.name("emitted", componentId, taskId));
        completeLatency = registry.timer(MetricRegistry.name("complete-latency", componentId, taskId));
    }

    @Override
    public void emit(EmitInfo info) {
        emittedTuples.mark();
    }

    @Override
    public void spoutAck(SpoutAckInfo info) {
        if (info.completeLatencyMs != null) {
            completeLatency.update(info.completeLatencyMs, TimeUnit.MILLISECONDS);
        }
    }
}
