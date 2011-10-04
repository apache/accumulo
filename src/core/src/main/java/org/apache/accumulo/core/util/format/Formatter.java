package org.apache.accumulo.core.util.format;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;


public interface Formatter extends Iterator<String>
{
    public void initialize(Iterable<Entry<Key, Value>> scanner, boolean printTimestamps);
}
