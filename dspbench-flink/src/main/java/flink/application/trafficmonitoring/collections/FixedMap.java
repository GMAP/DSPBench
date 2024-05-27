package flink.application.trafficmonitoring.collections;

/**
 * Author: Thilina
 * Date: 12/6/14
 * Credit : http://amix.dk/blog/post/19465
 */
import java.util.LinkedHashMap;
import java.util.Map;

public class FixedMap<K,V> extends LinkedHashMap<K,V> {
    private final int max_capacity;

    public FixedMap(int initial_capacity, int max_capacity) {
        super(initial_capacity, 0.75f, false);
        this.max_capacity = max_capacity;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K,V> eldest) {
        return size() > this.max_capacity;
    }

}