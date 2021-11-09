package com.streamer.utils;

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

/**
 *
 * @author mayconbordin
 */
public class HashUtils {
    private static final HashFunction MURMUR3 = Hashing.murmur3_128();
    
    public static final long murmurhash3(String value) {
        HashCode hc = MURMUR3.newHasher().putString(value, Charsets.UTF_8).hash();
        return hc.asLong();
    }
    
    public static final long murmurhash3(Object...values) {
        Hasher h = MURMUR3.newHasher();
        
        for (Object value : values) {
            if (value instanceof String)
                h.putString((String)value, Charsets.UTF_8);
            else if (value instanceof Integer)
                h.putInt((Integer)value);
            else if (value instanceof Long)
                h.putLong((Long)value);
            else if (value instanceof Double)
                h.putDouble((Double)value);
        }
 
        return h.hash().asLong();
    }
    
    public static final long murmurhash3(String...values) {
        Hasher h = MURMUR3.newHasher();
        
        for (String value : values) {
            h.putString(value, Charsets.UTF_8);
        }
 
        return h.hash().asLong();
    }
}
