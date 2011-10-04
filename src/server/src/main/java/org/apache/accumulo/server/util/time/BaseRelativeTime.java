package org.apache.accumulo.server.util.time;

/**
 * Provide time from a local source and a hint from a time source.
 * 
 * RelativeTime and BaseRelativeTime are separated to provide unit tests of the core functionality of Relative timekeeping.
 *
 */
public class BaseRelativeTime implements ProvidesTime {
    
    private long diff = 0;
    private long lastReportedTime = 0;
    ProvidesTime local;
    
    BaseRelativeTime(ProvidesTime real, long lastReportedTime) {
        this.local = real;
        this.lastReportedTime = lastReportedTime;
    }

    BaseRelativeTime(ProvidesTime real) {
        this(real, 0);
    }

    @Override
    synchronized public long currentTime() {
        long localNow = local.currentTime();
        long result = localNow + diff;
        if (result < lastReportedTime)
            return lastReportedTime;
        lastReportedTime = result;
        return result;
    }
    
    synchronized public void updateTime(long advice) {
        long localNow = local.currentTime();
        long diff = advice - localNow;
        // smooth in 20% of the change, not the whole thing.
        this.diff = (this.diff * 4 / 5) + diff / 5; 
    }

}
