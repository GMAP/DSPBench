package com.streamer.core.impl;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import com.streamer.core.Schema;
import com.streamer.core.Source;
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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import org.mockito.Mockito;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 *
 * @author mayconbordin
 */
public class StormSpoutTest {
    private StormSpout spout;
    private Source source;
    private SpoutOutputCollector collector;
    private Map<String, Stream> streams;
    
    @Before
    public void setUp() {
        source = mock(Source.class);
        collector = mock(SpoutOutputCollector.class);
        
        streams = new HashMap<String, Stream>();
        Stream stream = mock(Stream.class);
        
        when(stream.getSchema()).thenReturn(new Schema("id", "name").asKey("id"));
        streams.put("stream", stream);
        
        when(source.getOutputStreams()).thenReturn(streams);
        when(source.hasNext()).thenReturn(Boolean.TRUE);
        
        spout = new StormSpout();
        spout.setSource(source);
    }

    @Test
    public void testDeclareOutputFields() {
        OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);
        spout.declareOutputFields(declarer);
        
        final InOrder inOrder = Mockito.inOrder(declarer, source);
        inOrder.verify(source).getOutputStreams();
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

    /**
     * Test of open method, of class StormSpout.
     */
    @Test
    public void testOpen() {
        spout.open(new HashMap(), null, collector);
        
        final InOrder inOrder = Mockito.inOrder(source);
        inOrder.verify(source).onCreate(anyInt(), any(Configuration.class));
        inOrder.verify(source).setOutputStreams(anyMap());
    }

    /**
     * Test of nextTuple method, of class StormSpout.
     */
    @Test
    public void testNextTuple() {
        spout.nextTuple();
        
        final InOrder inOrder = Mockito.inOrder(source);
        inOrder.verify(source).hasNext();
        inOrder.verify(source).hooksBefore(null);
        inOrder.verify(source).nextTuple();
        inOrder.verify(source).hooksAfter(null);
    }
    
}
