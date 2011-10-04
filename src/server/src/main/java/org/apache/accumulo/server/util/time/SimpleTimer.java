package org.apache.accumulo.server.util.time;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Generic singleton timer: don't use it if you are going to do anything that will take very long. Please use it 
 * to reduce the number of threads dedicated to simple events.
 *
 */
public class SimpleTimer {
    
    private static SimpleTimer instance;
    private Timer timer;
    
    public static synchronized SimpleTimer getInstance() {
        if (instance == null)
            instance = new SimpleTimer();
        return instance;
    }

    private SimpleTimer() {
        timer = new Timer("SimpleTimer", true);
    }
    
    public void schedule(TimerTask task, long delay) {
        timer.schedule(task, delay);
    }
    
    public void schedule(TimerTask task, long delay, long period) {
        timer.schedule(task, delay, period);
    }
    
}
