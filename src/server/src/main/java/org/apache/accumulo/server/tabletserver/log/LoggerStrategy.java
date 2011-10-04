package org.apache.accumulo.server.tabletserver.log;

import java.util.Set;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;


public abstract class LoggerStrategy {
    // Called by the tablet server to get the list of loggers to use from the available set
    public abstract Set<String> getLoggers(Set<String> allLoggers);
    // Called by the master (via the tablet server) to prefer loggers for balancing
    public abstract void preferLoggers(Set<String> preference);
    
    public int getNumberOfLoggersToUse() {
        return AccumuloConfiguration.getSystemConfiguration().getCount(Property.TSERV_LOGGER_COUNT);
    }
}
