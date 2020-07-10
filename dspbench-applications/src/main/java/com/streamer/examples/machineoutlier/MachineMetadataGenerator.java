package com.streamer.examples.machineoutlier;

import com.streamer.base.source.generator.Generator;
import com.streamer.core.Values;
import com.streamer.examples.machineoutlier.MachineOutlierConstants.Config;
import com.streamer.utils.Configuration;
import java.util.Random;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class MachineMetadataGenerator extends Generator {
    private Random rand;
    
    private int numMachines;
    private int count = 0;
    private long currentTimestamp = 0;
    private String[] ipAddresses;
    
    @Override
    public Values generate() {
        if (count % numMachines == 0) {
            currentTimestamp = System.currentTimeMillis();
        }
        
        String ip = ipAddresses[count++ % numMachines];
        
        MachineMetadata metadata = new MachineMetadata();
        metadata.setTimestamp(currentTimestamp);
        metadata.setMachineIP(ip);
        metadata.setMemoryUsage(getRandomBetween(0, 100));
        metadata.setCpuUsage(getRandomBetween(0, 100));
        
        int id = String.format("%s:%s", ip, currentTimestamp).hashCode();
        
        Values values = new Values(metadata.getTimestamp(), metadata.getMachineIP(), metadata);
        values.setId(id);
        return values;
    }

    @Override
    public void initialize(Configuration config) {
        super.initialize(config);
        
        numMachines = config.getInt(Config.GENERATOR_NUM_MACHINES, 100);
        
        rand = new Random(System.currentTimeMillis());
        ipAddresses = new String[numMachines];
        
        for (int i=0; i<numMachines; i++) {
            ipAddresses[i] = getRandomIP();
        }
    }
    
    private double getRandomBetween(double min, double max) {
        return min + rand.nextDouble() * (max - min);
    }
    
    private String getRandomIP() {
        return rand.nextInt(256) + "." + rand.nextInt(256) + "." + rand.nextInt(256) + "." + rand.nextInt(256);
    }
}