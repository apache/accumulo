package org.apache.accumulo.server.master;

import org.apache.accumulo.server.util.DefaultMap;
import org.junit.Test;


import static org.junit.Assert.*;

public class DefaultMapTest {
    
    @Test
    public void testDefaultMap() {
        DefaultMap<String, String> map = new DefaultMap<String, String>("");
        map.put("key", "value");
        String empty = map.get("otherKey");
        assertEquals(map.get("key"), "value");
        assertEquals(empty, "");
        assertTrue(empty == map.get("otherKey"));
    }

}
