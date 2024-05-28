package flink.source;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import flink.constants.ReinforcementLearnerConstants;
import flink.util.RandomUtil;

public class RewardSource extends RichParallelSourceFunction<Tuple2<String, Integer>> {
    private volatile boolean isRunning = true;

    private final Map<String, Integer> actionSel = new HashMap<>();
    private Integer actionSelCountThreshold = 50;
    //actionCtrDistr = {'page1' : (30, 12), 'page2' : (60, 30), 'page3' : (80, 10)}
    private Map<String, List<Integer>> actionCtrDistr = Map.of(
            "page1", List.of(30, 12),
            "page2", List.of(60, 30),
            "page3", List.of(80, 10)
    );
    List<String> actions = Arrays.asList("page1", "page2", "page3");
    Random rand = new Random();
    private long maxRounds;

    public RewardSource(Configuration config) {
        maxRounds = config.getLong(ReinforcementLearnerConstants.Conf.GENERATOR_MAX_ROUNDS,10000);
    }

    @Override
    public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
        int count = 0;
        while (count <= maxRounds) {
            String action = actions.get(rand.nextInt(actions.size()));

            actionSel.put(action, actionSel.getOrDefault(action, 0) + 1);

            if (actionSel.get(action).equals(actionSelCountThreshold)) {
                count += 1;
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

                    ctx.collect(new Tuple2<String, Integer>(action, r2));
                }
            }
        }
        isRunning = false;
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
