package com.streamer.topology;

import com.streamer.core.Operator;
import com.streamer.core.Schema;
import com.streamer.core.Source;
import com.streamer.core.Stream;
import com.streamer.partitioning.Fields;
import com.streamer.partitioning.PartitioningScheme;
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Matchers;
import org.mockito.Mockito;
import static org.mockito.Mockito.*;

/**
 *
 * @author mayconbordin
 */
public class TopologyBuilderTest {
    private TopologyBuilder builder;
    private Topology topology;
    private ComponentFactory factory;
    
    @Before
    public void setUp() {
        factory = mock(ComponentFactory.class);
        topology = mock(Topology.class);
        
        when(factory.createTopology(Matchers.anyString())).thenReturn(topology);
        when(factory.createOperatorAdapter(anyString(), any(Operator.class))).thenReturn(mock(IOperatorAdapter.class));
        when(factory.createSourceAdapter(anyString(), any(Source.class))).thenReturn(mock(ISourceAdapter.class));
        when(factory.createStream(anyString(), any(Schema.class))).thenReturn(mock(Stream.class));
        
        builder = new TopologyBuilder();
        builder.setFactory(factory);
        
        builder.initTopology("test");
    }

    /**
     * Test of initTopology method, of class TopologyBuilder.
     */
    @Test
    public void testInitTopology() {
        ComponentFactory factory = mock(ComponentFactory.class);
        builder.setFactory(factory);
        builder.initTopology("test");
        
        verify(factory).createTopology("test");
    }

    /**
     * Test of build method, of class TopologyBuilder.
     */
    @Test
    public void testBuild() {
        builder.build();
        
        final InOrder inOrder = inOrder(topology);
        inOrder.verify(topology).finish();
    }

    /**
     * Test of createStream method, of class TopologyBuilder.
     */
    @Test
    public void testCreateStream() {
        Schema schema = new Schema();
        builder.createStream("test", schema);
        
        final InOrder inOrder = inOrder(factory, topology);
        inOrder.verify(factory).createStream("test", schema);
        inOrder.verify(topology).addStream(anyString(), any(Stream.class));
    }

    /**
     * Test of setSource method, of class TopologyBuilder.
     */
    @Test
    public void testSetSource() {
        Source source = mock(Source.class);        
        builder.setSource("testSource", source, 1);
        
        final InOrder inOrder = inOrder(source, factory, topology);
        inOrder.verify(source).setName("testSource");
        inOrder.verify(source).setParallelism(1);
        inOrder.verify(factory).createSourceAdapter("testSource", source);
        inOrder.verify(topology).addComponent(anyString(), any(ISourceAdapter.class));
    }

    /**
     * Test of setOperator method, of class TopologyBuilder.
     */
    @Test
    public void testSetOperator() {
        Operator op = mock(Operator.class);
        builder.setOperator("testOperator", op, 1);
        
        final InOrder inOrder = inOrder(op, factory, topology);
        inOrder.verify(op).setName("testOperator");
        inOrder.verify(op).setParallelism(1);
        inOrder.verify(factory).createOperatorAdapter("testOperator", op);
        inOrder.verify(topology).addComponent(anyString(), any(IOperatorAdapter.class));
    }

    /**
     * Test of publish method, of class TopologyBuilder.
     */
    @Test
    public void testPublish() {
        Stream stream = mock(Stream.class);
        Operator operator = mock(Operator.class);
        IOperatorAdapter adapter = mock(IOperatorAdapter.class);
        
        when(topology.getComponent(anyString())).thenReturn(adapter);
        when(adapter.getComponent()).thenReturn(operator);
        when(stream.getStreamId()).thenReturn("testStream");
        
        builder.publish("testOperator", stream);
        
        final InOrder inOrder = inOrder(stream, operator, adapter, factory, topology);
        inOrder.verify(topology).getComponent("testOperator");
        inOrder.verify(adapter).getComponent();
        inOrder.verify(operator).addOutputStream(stream);
        inOrder.verify(adapter).getComponent();
        inOrder.verify(operator).addOutputStream("testStream", stream);
        inOrder.verify(stream).addPublisher(operator);
    }

    /**
     * Test of shuffle method, of class TopologyBuilder.
     */
    @Test
    public void testSubscribe() {
        Stream stream = mock(Stream.class);
        Operator operator = mock(Operator.class);
        IOperatorAdapter adapter = mock(IOperatorAdapter.class);
        
        when(topology.getComponent(anyString())).thenReturn(adapter);
        when(adapter.getComponent()).thenReturn(operator);
        when(stream.getStreamId()).thenReturn("testStream");
        
        builder.shuffle("testOperator", stream);
        final InOrder inOrderShuffle = inOrder(stream, operator, adapter, factory, topology);
        inOrderShuffle.verify(topology).getComponent("testOperator");
        inOrderShuffle.verify(stream).addSubscriber("testOperator", adapter, PartitioningScheme.SHUFFLE, null);
        inOrderShuffle.verify(adapter).getComponent();
        inOrderShuffle.verify(operator).addInputStream(stream);
        
        builder.bcast("testOperator", stream); 
        final InOrder inOrderBcast = inOrder(stream, operator, adapter, factory, topology);
        inOrderBcast.verify(topology).getComponent("testOperator");
        inOrderBcast.verify(stream).addSubscriber("testOperator", adapter, PartitioningScheme.BROADCAST, null);
        inOrderBcast.verify(adapter).getComponent();
        inOrderBcast.verify(operator).addInputStream(stream);
        
        Fields fields = new Fields();
        builder.groupBy("testOperator", stream, fields);
        final InOrder inOrderGroupBy = inOrder(stream, operator, adapter, factory, topology);
        inOrderGroupBy.verify(topology).getComponent("testOperator");
        inOrderGroupBy.verify(stream).addSubscriber("testOperator", adapter, PartitioningScheme.GROUP_BY, fields);
        inOrderGroupBy.verify(adapter).getComponent();
        inOrderGroupBy.verify(operator).addInputStream(stream);
        
        builder.groupByKey("testOperator", stream);
        final InOrder inOrderGroupByKey = inOrder(stream, operator, adapter, factory, topology);
        inOrderGroupByKey.verify(topology).getComponent("testOperator");
        inOrderGroupByKey.verify(stream).addSubscriber("testOperator", adapter, PartitioningScheme.GROUP_BY, null);
        inOrderGroupByKey.verify(adapter).getComponent();
        inOrderGroupByKey.verify(operator).addInputStream(stream);
    }
    
    /**
     * Test of setTupleRate method, of class TopologyBuilder.
     */
    @Test
    public void testSetTupleRate() {
        ISourceAdapter adapter = mock(ISourceAdapter.class);
        
        when(topology.getComponent(anyString())).thenReturn(adapter);
        
        builder.setTupleRate("testSource", 1000);
        
        final InOrder inOrder = inOrder(adapter, factory, topology);
        inOrder.verify(topology).getComponent("testSource");
        inOrder.verify(adapter).setTupleRate(1000);
    }

    /**
     * Test of setTimer method, of class TopologyBuilder.
     */
    @Test
    public void testSetTimer() {
        IOperatorAdapter adapter = mock(IOperatorAdapter.class);
        
        when(topology.getComponent(anyString())).thenReturn(adapter);
        
        builder.setTimer("testOperator", 10, TimeUnit.SECONDS);
        
        final InOrder inOrder = inOrder(adapter, factory, topology);
        inOrder.verify(topology).getComponent("testOperator");
        inOrder.verify(adapter).setTimeInterval(10, TimeUnit.SECONDS);
    }
}
