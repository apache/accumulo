package org.apache.accumulo.core.util;

import org.apache.hadoop.conf.Configuration;

public class CachedConfiguration {
    private static Configuration configuration = null;
    public synchronized static Configuration getInstance() {
        if (configuration == null)
            setInstance(new Configuration());
        return configuration;
    }
    public synchronized static Configuration setInstance(Configuration update) {
        Configuration result = configuration;
        configuration = update;
        return result;
    }
}
