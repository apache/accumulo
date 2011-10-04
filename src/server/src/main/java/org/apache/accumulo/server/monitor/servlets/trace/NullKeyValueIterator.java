package org.apache.accumulo.server.monitor.servlets.trace;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;


public class NullKeyValueIterator implements Iterator<Entry<Key, Value>> {

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Entry<Key, Value> next() {
        return null;
    }

    @Override
    public void remove() {
    }
}
