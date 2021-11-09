package org.dspbench.sink.formatter;

import org.apache.storm.tuple.Tuple;
import org.dspbench.model.metadata.MachineMetadata;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class MachineMetadataFormatter extends Formatter {

    @Override
    public String format(Tuple tuple) {
        MachineMetadata metadata = (MachineMetadata) tuple.getValue(4);
        
        String line = "\"" + tuple.getValue(0) + "\"," + tuple.getValue(1) + ","
                    + tuple.getValue(2) + ",\"" + tuple.getValue(3) + "\","
                    + metadata.getCpuIdleTime() + "," + metadata.getFreeMemoryPercent();
        
        return line;
    }
    
}
