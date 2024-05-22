package flink.parsers;

import flink.application.machineoutiler.MachineMetadata;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlibabaMachineUsageParser extends Parser
        implements MapFunction<String, Tuple3<String, Long, MachineMetadata>> {

    private static final Logger LOG = LoggerFactory.getLogger(AlibabaMachineUsageParser.class);

    Configuration config;

    public AlibabaMachineUsageParser(Configuration config) {
        super.initialize(config);
        this.config = config;
    }

    @Override
    public Tuple3<String, Long, MachineMetadata> map(String value) throws Exception {
        super.initialize(config);
        super.incBoth();
        String[] temp = value.split(",");
        if(temp[0] == null || temp[1] == null || temp[2] == null || temp[3] == null){
            return null;
        }
        else{
            return new Tuple3<>(
                    temp[0],
                    Long.parseLong(temp[1]) * 1000,
                    new MachineMetadata(Long.parseLong(temp[1]) * 1000, temp[0], Double.parseDouble(temp[2]),
                            Double.parseDouble(temp[3])));
        }
    }

    @Override
    public Tuple1<?> parse(String input) {
        return null;
    }
}
