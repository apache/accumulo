package org.apache.accumulo.core.client.mock;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;


class MockConfiguration extends AccumuloConfiguration {
    Map<String, String> map;
    
    MockConfiguration(Map<String, String> settings) {
        map = settings;
    }
    
    public void put(String k, String v) { 
        map.put(k, v);
    }
    @Override
    public String get(Property property) {
        return map.get(property.getKey());
    }
    @Override
    public Iterator<Entry<String, String>> iterator() {
        return map.entrySet().iterator();
    }
}