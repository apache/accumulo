package org.apache.accumulo.server.util;


import static org.junit.Assert.*;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.server.util.DefaultMap;
import org.junit.Test;


public class DefaultMapTest {
    
    @Test
    public void testDefaultMap() {
        Integer value = new DefaultMap<String, Integer>(0).get("test");
        assertNotNull(value);
        assertEquals(new Integer(0), value);
        value = new DefaultMap<String, Integer>(1).get("test");
        assertNotNull(value);
        assertEquals(new Integer(1), value);
        
        AtomicInteger canConstruct = new DefaultMap<String, AtomicInteger>(new AtomicInteger(1)).get("test");
        assertNotNull(canConstruct);
        assertEquals(new AtomicInteger(0).get(), canConstruct.get());
        
        DefaultMap<String, String> map = new DefaultMap<String, String>("");
        assertEquals(map.get("foo"), "");
        map.put("foo", "bar");
        assertEquals(map.get("foo"), "bar");
    }

}
