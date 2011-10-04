package org.apache.accumulo.server.util;

import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.log4j.Logger;


public class Halt {
    static private Logger log = Logger.getLogger(Halt.class);
    
    public static void halt(final String msg) {
        halt(0, new Runnable() {
            public void run() {
                log.fatal(msg);
            }
        });
    }

    public static void halt(final String msg, int status) {
        halt(status, new Runnable() {
            public void run() {
                log.fatal(msg);
            }
        });
    }

    public static void halt(final int status, Runnable runnable) {
        try {
            // give ourselves a little time to try and do something
            new Daemon() {
                public void run() {
                    UtilWaitThread.sleep(100);
                    Runtime.getRuntime().halt(status);
                }
            }.start();
            
            if (runnable != null)
                runnable.run();
            Runtime.getRuntime().halt(status);
        } finally {
            // In case something else decides to throw a Runtime exception
            Runtime.getRuntime().halt(-1);
        }
    }

}
