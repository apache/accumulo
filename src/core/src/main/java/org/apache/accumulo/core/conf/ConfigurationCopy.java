package org.apache.accumulo.core.conf;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class ConfigurationCopy extends AccumuloConfiguration {
    final Map<String, String> copy = Collections.synchronizedMap(new HashMap<String, String>());
    
    public ConfigurationCopy(Map<String, String> config) {
        this(config.entrySet());
    }
    
    public ConfigurationCopy(Iterable<Entry<String, String>> config) {
        for (Entry<String, String> entry : config) {
            copy.put(entry.getKey(), entry.getValue());
        }
    }

    public ConfigurationCopy() {
        this(new HashMap<String, String>());
    }

    @Override
    public String get(Property property) {
        return copy.get(property.getKey());
    }

    @Override
    public Iterator<Entry<String, String>> iterator() {
        return copy.entrySet().iterator();
    }

    public void set(Property prop, String value) {
        copy.put(prop.getKey(), value);
    }

    public void set(String key, String value) {
        copy.put(key, value);
    }

}
