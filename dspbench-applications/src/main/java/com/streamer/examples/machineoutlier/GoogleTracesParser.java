package com.streamer.examples.machineoutlier;

import com.google.common.collect.ImmutableList;
import com.streamer.base.source.parser.Parser;
import com.streamer.core.Values;
import com.streamer.examples.machineoutlier.MachineMetadata;
import com.streamer.utils.HashUtils;
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
        
        return ImmutableList.of(values);
    }
}