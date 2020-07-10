package com.streamer.core.impl;

import backtype.storm.topology.BoltDeclarer;
import com.streamer.core.Operator;
import com.streamer.core.Schema;
import com.streamer.core.Tuple;
import com.streamer.partitioning.Fields;
import com.streamer.partitioning.PartitioningScheme;
import com.streamer.topology.IOperatorAdapter;
import com.streamer.topology.impl.StormOperatorAdapter;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import static org.mockito.Mockito.*;

/**
 *
 * @author mayconbordin
 */
public class StormStreamTest {
    private StormStream stream;
    private BoltDeclarer declarer;
    private Schema schema;
    private StormOperatorAdapter adapter;
    
    @Before
    public void setUp() {
        declarer = mock(BoltDeclarer.class);
        adapter = mock(StormOperatorAdapter.class);
        
        when(adapter.getDeclarer()).thenReturn(declarer);
        
        schema = new Schema().keys("id").fields("name");
        stream = new StormStream("stream", schema);
        
        Operator operator = mock(Operator.class);
        when(operator.getName()).thenReturn("operator");
        stream.addPublisher(operator);
    }
    
    @Test
    public void testAddSubscriberShuffle() {
        stream.addSubscriber("test", adapter, PartitioningScheme.SHUFFLE);
        
        verify(adapter).getDeclarer();
        verify(declarer).shuffleGrouping("operator", "stream");
    }
    
    @Test
    public void testAddSubscriberBroadcast() {
        stream.addSubscriber("test", adapter, PartitioningScheme.BROADCAST);
        
        verify(adapter).getDeclarer();
        verify(declarer).allGrouping("operator", "stream");
    }
    
    @Test
    public void testAddSubscriberGroupBy() {
        Fields fields = new Fields("id", "name");
        stream.addSubscriber("test", adapter, PartitioningScheme.GROUP_BY, fields);
        
        ArgumentCaptor<String> arg1 = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> arg2 = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<backtype.storm.tuple.Fields> arg3 = ArgumentCaptor.forClass(backtype.storm.tuple.Fields.class);
        
        verify(adapter).getDeclarer();
        verify(declarer).fieldsGrouping(arg1.capture(), arg2.capture(), arg3.capture());
        
        assertEquals("operator", arg1.getValue());
        assertEquals("stream", arg2.getValue());
        assertArrayEquals(fields.toArray(), arg3.getValue().toList().toArray());
    }
    
    @Test
    public void testAddSubscriberGroupByKey() {
        stream.addSubscriber("test", adapter, PartitioningScheme.GROUP_BY);
        
        ArgumentCaptor<String> arg1 = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> arg2 = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<backtype.storm.tuple.Fields> arg3 = ArgumentCaptor.forClass(backtype.storm.tuple.Fields.class);
        
        verify(adapter).getDeclarer();
        verify(declarer).fieldsGrouping(arg1.capture(), arg2.capture(), arg3.capture());
        
        assertEquals("operator", arg1.getValue());
        assertEquals("stream", arg2.getValue());
        assertArrayEquals(new String[]{"id"}, arg3.getValue().toList().toArray());
    }
    
    
    @Test(expected = RuntimeException.class)
    public void testPut() {
        stream.put(mock(Operator.class), mock(Tuple.class));
    }
}
