package com.streamer.core;

import com.streamer.core.hook.Hook;
import java.util.Map;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 *
 * @author mayconbordin
 */
public class ComponentTest {
    
    @Test
    public void initializeComponent() {
        Component c = new Component();
        c.setName("component");
        c.onCreate(1, null);
        
        assertEquals(c.getFullName(), "component-1");
    }
    
    @Test
    public void testStreamMethods() {
        Component c = new Component();
        
        Stream s = mock(Stream.class);
        c.addOutputStream(s);
        
        assertEquals(c.getDefaultOutputStream(), s);
        
        Stream s2 = mock(Stream.class);
        c.addOutputStream("stream-2", s2);
        
        Map<String, Stream> streams = c.getOutputStreams();
        assertEquals(streams.get(Constants.DEFAULT_STREAM), s);
        assertEquals(streams.get("stream-2"), s2);
        
        assertEquals(streams.size(), 2);
    }
    
    @Test
    public void testHooks() {
        Component c = new Component();
        Tuple tuple = mock(Tuple.class);
        
        Hook h1 = mock(Hook.class);
        Hook h2 = mock(Hook.class);
        
        when(h1.isHighPriority()).thenReturn(Boolean.TRUE);
        when(h1.isLowPriority()).thenReturn(Boolean.FALSE);
        when(h2.isLowPriority()).thenReturn(Boolean.TRUE);
        when(h2.isHighPriority()).thenReturn(Boolean.FALSE);

        c.addHook(h1);
        c.addHook(h2);
        c.hooksBefore(tuple);
        
        assertTrue(c.getHooks().size() == 2);
        
        final InOrder inOrder = Mockito.inOrder(h2, h1);
        inOrder.verify(h2).beforeTuple(tuple);
        inOrder.verify(h1).beforeTuple(tuple);
    }
    
    @Test
    public void testEmit() {
        Component c = new Component();
        Values v = new Values();
        Tuple parent = mock(Tuple.class);
        
        Hook h1 = mock(Hook.class);
        Hook h2 = mock(Hook.class);
        
        when(h1.isHighPriority()).thenReturn(Boolean.TRUE);
        when(h1.isLowPriority()).thenReturn(Boolean.FALSE);
        when(h2.isLowPriority()).thenReturn(Boolean.TRUE);
        when(h2.isHighPriority()).thenReturn(Boolean.FALSE);

        c.addHook(h1);
        c.addHook(h2);
        
        Stream s1 = mock(Stream.class);
        Stream s2 = mock(Stream.class);
        
        doNothing().when(s1).put(c, v);
        
        c.addOutputStream(s1);
        c.addOutputStream("stream-1", s1);
        c.addOutputStream("stream-2", s2);
        
        c.emit("stream-1", v);
        c.emit("stream-2", parent, v);
        c.emit(v);
        
        verify(s1, times(2)).put(c, v);
        verify(s2).put(c, parent, v);
        
        final InOrder inOrder = Mockito.inOrder(h2, h1);
        inOrder.verify(h2).onEmit(v);
        inOrder.verify(h1).onEmit(v);
    }
    
}
