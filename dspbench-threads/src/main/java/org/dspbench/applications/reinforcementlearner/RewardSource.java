package org.dspbench.applications.reinforcementlearner;

import org.dspbench.base.source.BaseSource;
import org.dspbench.core.Values;
import org.dspbench.utils.RandomUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 *
 * @author mayconbordin
 */
public class RewardSource extends BaseSource {
    private static final Logger LOG = LoggerFactory.getLogger(RewardSource.class);

    private final Map<String, Integer> actionSel = new HashMap<>();
    private Integer actionSelCountThreshold = 50;
    //actionCtrDistr = {'page1' : (30, 12), 'page2' : (60, 30), 'page3' : (80, 10)}
    private Map<String, List<Integer>> actionCtrDistr = Map.of(
            "page1", List.of(30, 12),
            "page2", List.of(60, 30),
            "page3", List.of(80, 10)
    );

    @Override
    protected void initialize() {

    }

    @Override
    public void nextTuple() {
        try {
            while (true) {
                String action = SharedQueue.QUEUE.take();
                LOG.info("Received action {} from queue", action);

                actionSel.put(action, actionSel.getOrDefault(action, 0) + 1);

                if (actionSel.get(action).equals(actionSelCountThreshold)) {
                    List<Integer> distr = actionCtrDistr.get(action);
                    int sum = 0;

                    for (int i=0; i<12; i++) {
                        sum = sum + RandomUtil.randInt(1, 100);

                        double r = (sum - 100) / 100.0;
                        int r2 = (int) r * distr.get(1) + distr.get(0);

                        if (r2 < 0) {
                            r2 = 0;
                        }

                        actionSel.put(action, 0);

                        LOG.info("Sending action {} with reward {}", action, r2);
                        emit(new Values(action, r2));
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public boolean hasNext() {
        return true;
    }
}
