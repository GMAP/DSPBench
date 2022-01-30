package org.dspbench.applications.machineoutlier;

import org.dspbench.base.source.parser.Parser;
import org.dspbench.core.Values;
import org.dspbench.utils.HashUtils;

import java.util.List;

/**
 *
 * ### machine usage
 * +--------------------------------------------------------------------------------------------+
 * | Field            | Type       | Label | Comment                                            |
 * +--------------------------------------------------------------------------------------------+
 * | machine_id       | string     |       | uid of machine                                     |
 * | time_stamp       | double     |       | time stamp, in second                              |
 * | cpu_util_percent | bigint     |       | [0, 100]                                           |
 * | mem_util_percent | bigint     |       | [0, 100]                                           |
 * | mem_gps          | double     |       | normalized memory bandwidth, [0, 100]              |
 * | mkpi             | bigint     |       | cache miss per thousand instruction                |
 * | net_in           | double     |       | normarlized in coming network traffic, [0, 100]    |
 * | net_out          | double     |       | normarlized out going network traffic, [0, 100]    |
 * | disk_io_percent  | double     |       | [0, 100], abnormal values are of -1 or 101         |
 * +--------------------------------------------------------------------------------------------+
 * from: https://github.com/alibaba/clusterdata/blob/master/cluster-trace-v2018/schema.txt
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class AlibabaMachineUsageParser extends Parser {
    private static final int TIMESTAMP  = 1;
    private static final int MACHINE_ID = 0;
    private static final int CPU        = 2;
    private static final int MEMORY     = 3;
    
    @Override
    public List<Values> parse(String str) {
        String[] items = str.split(",");
        
        if (items.length != 9)
            return null;
        
        String id      = items[MACHINE_ID];
        long timestamp = Long.parseLong(items[TIMESTAMP]) * 1000;
        double cpu     = Double.parseDouble(items[CPU]);
        double memory  = Double.parseDouble(items[MEMORY]);
        
        Values values = new Values();
        values.add(id);
        values.add(timestamp);
        values.add(new MachineMetadata(timestamp, id, cpu, memory));
        values.setId(HashUtils.murmurhash3(String.format("%s:%s", id, timestamp)));
        
        return List.of(values);
    }
}