package org.apache.accumulo.server.util.time;

/**
 * The most obvious implementation of ProvidesTime. 
 *
 */
public class SystemTime implements ProvidesTime {

    @Override
    public long currentTime() {
        return System.currentTimeMillis();
    }

}
