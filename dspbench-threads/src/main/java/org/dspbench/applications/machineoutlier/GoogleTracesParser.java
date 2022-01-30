package org.dspbench.applications.machineoutlier;

import org.dspbench.base.source.parser.Parser;
import org.dspbench.core.Values;
import org.dspbench.utils.HashUtils;

import java.util.List;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class GoogleTracesParser extends Parser {
    private static final int TIMESTAMP  = 0;
    private static final int MACHINE_ID = 4;
    private static final int CPU        = 5;
    private static final int MEMORY     = 6;
    
    @Override
    public List<Values> parse(String str) {
        String[] items = str.split(",");
        
        if (items.length != 19)
            return null;
        
        String id      = items[MACHINE_ID];
        long timestamp = Long.parseLong(items[TIMESTAMP]);
        double cpu     = Double.parseDouble(items[CPU]) * 10;
        double memory  = Double.parseDouble(items[MEMORY]) * 10;
        
        Values values = new Values();
        values.add(id);
        values.add(timestamp);
        values.add(new MachineMetadata(timestamp, id, cpu, memory));
        values.setId(HashUtils.murmurhash3(String.format("%s:%s", id, timestamp)));
        
        return List.of(values);
    }
}