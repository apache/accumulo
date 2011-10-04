package org.apache.accumulo.server.util.time;

/**
 * An interface for anything that returns the time in the same format as System.currentTimeMillis(). 
 *
 */
public interface ProvidesTime {
    public long currentTime();
}
