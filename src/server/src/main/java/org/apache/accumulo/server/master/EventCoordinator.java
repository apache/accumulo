package org.apache.accumulo.server.master;

import org.apache.log4j.Logger;

public class EventCoordinator {
    
    private static final Logger log = Logger.getLogger(EventCoordinator.class);
    
    synchronized public void waitForSomethingInterestingToHappen(long millis) {
        if (millis <= 0)
            return;
        try {
            wait(millis);
        } catch (InterruptedException e) {
            log.debug("ignoring InterruptedException", e);
        }
    }

    synchronized public void somethingInterestingHappened(String msg, Object... args) {
        log.info(String.format(msg, args));
        notifyAll();
    }
}