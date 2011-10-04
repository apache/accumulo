package org.apache.accumulo.server.util.time;

import static org.junit.Assert.*;

import org.apache.accumulo.server.util.time.BaseRelativeTime;
import org.apache.accumulo.server.util.time.ProvidesTime;
import org.junit.Test;


public class BaseRelativeTimeTest {
    
    static class BogusTime implements ProvidesTime {
        public long value = 0;
        public long currentTime() { return value; }
    }
    
    @Test
    public void testMatchesTime() {
        BogusTime bt = new BogusTime();
        BogusTime now = new BogusTime();
        now.value = bt.value = System.currentTimeMillis();
        
        BaseRelativeTime brt = new BaseRelativeTime(now);
        assertEquals(brt.currentTime(), now.value);
        brt.updateTime(now.value);
        assertEquals(brt.currentTime(), now.value);
    }
    
    @Test
    public void testFutureTime() {
        BogusTime advice = new BogusTime();
        BogusTime local = new BogusTime();
        local.value = advice.value = System.currentTimeMillis();
        // Ten seconds into the future
        advice.value += 10000; 
        
        
        BaseRelativeTime brt = new BaseRelativeTime(local);
        assertEquals(brt.currentTime(), local.value);
        brt.updateTime(advice.value);
        long once = brt.currentTime();
        assertTrue(once < advice.value);
        assertTrue(once > local.value);
        
        for (int i = 0; i < 100; i++) {
            brt.updateTime(advice.value);
        }
        long many = brt.currentTime();
        assertTrue(many > once);
        assertTrue("after much advice, relative time is still closer to local time", 
                   (advice.value - many) < (once - local.value) );
    }
    
    @Test
    public void testPastTime() {
        BogusTime advice = new BogusTime();
        BogusTime local = new BogusTime();
        local.value = advice.value = System.currentTimeMillis();
        // Ten seconds into the past
        advice.value -= 10000;
        
        BaseRelativeTime brt = new BaseRelativeTime(local);
        brt.updateTime(advice.value);
        long once = brt.currentTime();
        assertTrue(once < local.value);
        brt.updateTime(advice.value);
        long twice = brt.currentTime();
        assertTrue("Time cannot go backwards", once <= twice);
        brt.updateTime(advice.value - 10000);
        assertTrue("Time cannot go backwards", once <= twice);
    }

}
