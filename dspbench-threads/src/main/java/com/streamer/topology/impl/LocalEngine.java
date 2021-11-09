package com.streamer.topology.impl;

import com.streamer.topology.IOperatorAdapter;
import com.streamer.topology.ISourceAdapter;
import com.streamer.topology.Topology;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mayconbordin
 */
public class LocalEngine {
    private static final Logger LOG = LoggerFactory.getLogger(LocalEngine.class);
    
    private static LocalEngine engine;
    
    private LocalTopology topology;
    private List<ExecutorService> threadPool = new ArrayList<ExecutorService>();
    
    private LocalEngine() {}
    
    public static LocalEngine getEngine() {
        if (engine == null) {
            engine = new LocalEngine();
        }
        return engine;
    }
    
    public void submitTopology(Topology topology) {
        this.topology = (LocalTopology) topology;
        LOG.info("Topology {} submitted to LocalEngine", topology.getName());
    }
    
    public void run() {
        LOG.info("Running {} topology in LocalEngine", topology.getName());

        // setup operator instances
        for (IOperatorAdapter adapter : topology.getOperators()) {
            LOG.info("Initializing {} operator with {} threads", adapter.getComponent().getName(), adapter.getComponent().getParallelism());
            ((LocalOperatorAdapter)adapter).setupInstances();
            
            ExecutorService executor = Executors.newFixedThreadPool(adapter.getComponent().getParallelism());
            threadPool.add(executor);
            
            for (LocalOperatorInstance opInstance : ((LocalOperatorAdapter)adapter).getInstances()) {
                LOG.info("{}-{} initialized", adapter.getComponent().getName(), opInstance.getIndex());
                executor.execute(opInstance.getProcessRunner());
            }
            
            if (adapter.hasTimer()) {
                ScheduledExecutorService timeExecutor = Executors.newScheduledThreadPool(adapter.getComponent().getParallelism());
                threadPool.add(timeExecutor);
                
                for (LocalOperatorInstance opInstance : ((LocalOperatorAdapter)adapter).getInstances()) {
                    LOG.info("{}-{} scheduled at a rate of {} ms", adapter.getComponent().getName(), opInstance.getIndex(), adapter.getTimeIntervalMillis());
                    timeExecutor.scheduleAtFixedRate(opInstance.getTimeRunner(), 
                            0, adapter.getTimeIntervalMillis(), TimeUnit.MILLISECONDS);
                }
            }
        }
        
        // setup source instances
        for (ISourceAdapter adapter : topology.getSources()) {
            LOG.info("Initializing {} source with {} threads", adapter.getComponent().getName(), adapter.getComponent().getParallelism());
            ((LocalSourceAdapter)adapter).setupInstances();
            ExecutorService executor = Executors.newFixedThreadPool(adapter.getComponent().getParallelism());
            threadPool.add(executor);
            
            for (LocalSourceInstance srcInstance : ((LocalSourceAdapter)adapter).getInstances()) {
                executor.execute(srcInstance);
            }
        }
        
        LOG.info("Running...");
    }
}
