package com.streamer.metrics;

import com.codahale.metrics.Gauge;
import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.mutable.MutableLong;

/**
 *
 * @author mayconbordin
 */
public class Throughput implements Gauge<String> {
    private AtomicLong lastTime;
    private Map<Long, MutableLong> timeslots = new ConcurrentHashMap<Long, MutableLong>();
    
    public Context time() {
        return new Context(this);
    }

    public String getValue() {
        //StringBuilder sb = new StringBuilder("{");
        
        StringBuilder sb = new StringBuilder();
        //long total = 0;
        
        for (Long timestamp : timeslots.keySet()) {
            if (timestamp != lastTime.longValue()) {
                MutableLong count = timeslots.remove(timestamp);
                //total += count.longValue();
                sb.append(String.format("%d=%d;", timestamp, count.longValue()));
            }
        }
        sb.deleteCharAt(sb.length() - 1);
        //sb.append(String.format("total=%d", total));
        //sb.append("}");
        return sb.toString();
    }
        
    public static class Context implements Closeable {
        private final Throughput tp;

        private Context(Throughput tp) {
            this.tp = tp;
        }

        public void stop() {
            tp.update(System.currentTimeMillis()/1000);
        }

        @Override
        public void close() {
            stop();
        }
    }
    
    public void update(long currentTime) {   
        MutableLong count = timeslots.get(currentTime);
        if (count == null) {
            count = new MutableLong(1);
            timeslots.put(currentTime, count);
        }
        
        count.increment();
            
        if (lastTime == null)
            lastTime = new AtomicLong(currentTime);
        else
            lastTime.set(currentTime);
    }
    
    public static class TimeSlot {
        public long time;
        public long count = 0;
    }
}
