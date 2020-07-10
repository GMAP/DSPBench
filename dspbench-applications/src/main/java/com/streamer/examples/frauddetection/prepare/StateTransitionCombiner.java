package com.streamer.examples.frauddetection.prepare;

import com.streamer.examples.utils.Tuple;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class StateTransitionCombiner extends Reducer<Tuple, IntWritable, Tuple, IntWritable> {
    private int count;
    private IntWritable outVal = new IntWritable();

    @Override
    protected void reduce(Tuple  key, Iterable<IntWritable> values, Context context) 
            throws IOException, InterruptedException {
        count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }
        outVal.set(count);
        context.write(key, outVal);       	
    }
}	