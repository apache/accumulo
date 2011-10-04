package org.apache.accumulo.server.util.time;

/**
 * Provide time from System time and hints from another time source. 
 *
 * Provides a convenient static replacement for System.currentTimeMillis()
 */
public class RelativeTime extends BaseRelativeTime {
    
    private RelativeTime() { super(new SystemTime()); }
    
    private static BaseRelativeTime instance = new RelativeTime();
        
    public static BaseRelativeTime getInstance() { return instance; }
    
    public static void setInstance(BaseRelativeTime newInstance) { instance = newInstance; } 
        
    public static long currentTimeMillis() { return getInstance().currentTime(); }
    
}
