package com.streamer.topology.impl;

import com.streamer.core.Operator;
import com.streamer.core.Tuple;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalOperatorInstance {
    private static final Logger LOG = LoggerFactory.getLogger(LocalOperatorInstance.class);
    private final Operator operator;
    private final int index;
    private final BlockingQueue<Tuple> buffer;

    public LocalOperatorInstance(Operator operator, int index) {
        this.operator = operator;
        this.index = index;

        buffer = new LinkedBlockingQueue<Tuple>();
    }

    public Operator getOperator() {
        return operator;
    }

    public int getIndex() {
        return index;
    }

    public void processTuple(Tuple tuple) {
        try {
            buffer.put(tuple);
        } catch (InterruptedException ex) {
            LOG.error("Error in operator buffer", ex);
        }
    }

    public Runnable getProcessRunner() {
        return processRunner;
    }

    public Runnable getTimeRunner() {
        return timeRunner;
    }
    
    private final Runnable processRunner = new Runnable() {
        public void run() {
            while (true) {
                try {
                    Tuple tuple = buffer.take();

                    synchronized (operator) {
                        operator.hooksBefore(tuple);
                        operator.process(tuple);
                        operator.hooksAfter(tuple);
                    }
                } catch (InterruptedException ex) {
                    LOG.error("Error in operator buffer", ex);
                } catch (Exception ex) {
                    LOG.error("Unknown error: " + ex.getMessage(), ex);
                }
            }
        }
    };
    
    private final Runnable timeRunner = new Runnable() {
        public void run() {
            synchronized (operator) {
                try {
                    operator.onTime();
                } catch (Exception ex) {
                    LOG.error("An exception ocurred in the onTime method", ex);
                }
            }
        }
    };
}