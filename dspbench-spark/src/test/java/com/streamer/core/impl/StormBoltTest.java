package com.streamer.core.impl;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.streamer.core.Operator;
import com.streamer.core.Schema;
import com.streamer.core.Stream;
import com.streamer.topology.impl.StormConstants;
import com.streamer.utils.Configuration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Matchers;
import static org.mockito.Matchers.*;
import org.mockito.Mockito;
import static org.mockito.Mockito.*;

/**
 *
 * @author mayconbordin
 */
public class StormBoltTest {
    private StormBolt bolt;
    private Operator operator;
    private OutputCollector collector;
    private Map<String, Stream> streams;
    
    @Before
    public void setUp() {
        operator = mock(Operator.class);
        collector = mock(OutputCollector.class);
        
        streams = new HashMap<String, Stream>();
        Stream stream = mock(Stream.class);
        
        when(stream.getSchema()).thenReturn(new Schema("id", "name").asKey("id"));
        streams.put("stream", stream);
        
        when(operator.getOutputStreams()).thenReturn(streams);
        
        bolt = new StormBolt();
        bolt.setOperator(operator);
    }
    
    @Test
    public void testDeclareOutputStreams() {
        OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);
        bolt.declareOutputFields(declarer);
        
        final InOrder inOrder = Mockito.inOrder(declarer, operator);
        inOrder.verify(operator).getOutputStreams();
        inOrder.verify(declarer).declareStream(anyString(), any(Fields.class));
        
        ArgumentCaptor<Fields> arg = ArgumentCaptor.forClass(Fields.class);
        verify(declarer).declareStream(anyString(), arg.capture());
        
        List<String> keyFields = streams.get("stream").getSchema().getKeys();
        List<String> testFields = arg.getValue().toList();
        
        assertEquals(keyFields.size(), testFields.size() - 1);
        
        int i = 0;
        for (; i<keyFields.size(); i++) {
            assertEquals(keyFields.get(i), testFields.get(i));
        }
        
        assertEquals(StormConstants.TUPLE_FIELD, testFields.get(i));
    }

    
    @Test
    public void testPrepare() {
        bolt.prepare(new HashMap(), null, collector);
        
        final InOrder inOrder = Mockito.inOrder(operator);
        inOrder.verify(operator).onCreate(anyInt(), any(Configuration.class));
        inOrder.verify(operator).setOutputStreams(anyMap());
    }

    /**
     * Test of execute method, of class StormBolt.
     */
    @Test
    public void testExecute() {
        Tuple tuple = mock(Tuple.class);
        com.streamer.core.Tuple input = mock(com.streamer.core.Tuple.class);
        
        when(tuple.getValueByField(StormConstants.TUPLE_FIELD)).thenReturn(input);
        when(tuple.getSourceComponent()).thenReturn("component");
        when(tuple.getSourceStreamId()).thenReturn("stream");
        
        bolt.prepare(null, null, collector);
        bolt.execute(tuple);
        
        final InOrder inOrder = Mockito.inOrder(input, operator, collector);
        inOrder.verify(input).setTempValue(tuple);
        inOrder.verify(operator).hooksBefore(input);
        inOrder.verify(operator).process(input);
        inOrder.verify(operator).hooksAfter(input);
        inOrder.verify(collector).ack(tuple);
    }

    /**
     * Test of getComponentConfiguration method, of class StormBolt.
     */
    @Test
    public void testGetComponentConfiguration() {
        bolt.setTimeInterval(10);
        
        Map conf = bolt.getComponentConfiguration();
        
        assertTrue(conf.containsKey(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS));
        assertEquals(10L, conf.get(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS));
    }
    
}
