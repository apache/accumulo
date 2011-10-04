package org.apache.accumulo.server.tabletserver.log;

import static java.lang.Math.min;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.server.tabletserver.TabletServer;


public class RandomLoggerStrategy extends LoggerStrategy {
    
	public RandomLoggerStrategy() {
    }
    
	public RandomLoggerStrategy(TabletServer tserver) {
    }
    
    @Override
    public Set<String> getLoggers(Set<String> allLoggers) {
        List<String> copy = new ArrayList<String>(allLoggers);
        Collections.shuffle(copy);
        return new HashSet<String>(copy.subList(0, min(copy.size(), getNumberOfLoggersToUse())));
    }

    @Override
    public void preferLoggers(Set<String> preference) {
        // ignored
    }

}
