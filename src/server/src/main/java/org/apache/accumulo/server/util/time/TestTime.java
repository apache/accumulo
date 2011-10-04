package org.apache.accumulo.server.util.time;

import org.apache.log4j.Logger;

public class TestTime extends BaseRelativeTime {
    final long offset;
    private static Logger log = Logger.getLogger(TestTime.class);
    
    public TestTime(long millis) { super(new SystemTime()); offset = millis; }
    @Override
    public long currentTime() {
        log.debug("Using time offset of " + offset);
        return super.currentTime() + offset; 
    }
}
