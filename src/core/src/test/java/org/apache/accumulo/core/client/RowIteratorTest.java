package org.apache.accumulo.core.client;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Test;
import static org.junit.Assert.*;



public class RowIteratorTest {
    
    Iterator<Entry<Key, Value>> makeIterator(String ... args) {
        Map<Key, Value> result = new TreeMap<Key, Value>();
        for (String s : args) {
            String parts[] = s.split("[ \t]");
            Key key = new Key(parts[0], parts[1], parts[2]);
            Value value = new Value(parts[3].getBytes());
            result.put(key, value);
        }
        return result.entrySet().iterator();
    }
    
    List<List<Entry<Key, Value>>> getRows(Iterator<Entry<Key, Value>> iter) {
        List<List<Entry<Key, Value>>> result = new ArrayList<List<Entry<Key, Value>>>();
        RowIterator riter = new RowIterator(iter);
        while (riter.hasNext()) {
            result.add(riter.next());
        }
        return result; 
    }
    
    @Test
    public void testRowIterator() {
        List<List<Entry<Key, Value>>> rows = getRows(makeIterator());
        assertEquals(0, rows.size());
        rows = getRows(makeIterator("a b c d"));
        assertEquals(1, rows.size());
        assertEquals(1, rows.get(0).size());
        rows = getRows(makeIterator(
                "a cf cq1 v",
                "a cf cq2 v",
                "a cf cq3 v",
                "b cf cq1 x"
                ));
        assertEquals(2, rows.size());
        assertEquals(3, rows.get(0).size());
        assertEquals(1, rows.get(1).size());
        
        RowIterator i = new RowIterator(makeIterator());
        try {
            i.next();
            fail();
        } catch (NoSuchElementException ex) {
        }
        
        i = new RowIterator(makeIterator("a b c d", "a 1 2 3"));
        assertTrue(i.hasNext());
        i.next();
        assertFalse(i.hasNext());
        try {
            i.next();
            fail();
        } catch (NoSuchElementException ex) {
        }
    }
    

}
