package com.streamer.partitioning;

import java.util.ArrayList;
import java.util.Arrays;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class Fields extends ArrayList<String> {
    
    public Fields(String...fields) {
        super(Arrays.asList(fields));
    }
}
