package org.dspbench.applications.reinforcementlearner;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class SharedQueue {
    public static final BlockingQueue<String> QUEUE = new LinkedBlockingQueue<>();
}
