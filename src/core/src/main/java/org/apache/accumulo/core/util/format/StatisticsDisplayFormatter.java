package org.apache.accumulo.core.util.format;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;


/**
 * Does not show contents from scan, only displays statistics.
 * Beware that this work is being done client side and this was developed
 * as a utility for debugging. If used on large result sets it will likely
 * fail.
 */
public class StatisticsDisplayFormatter extends DefaultFormatter
{
    private Map<String, Long> classifications = new HashMap<String, Long>();
    private Map<String, Long> columnFamilies = new HashMap<String, Long>();
    private Map<String, Long> columnQualifiers = new HashMap<String, Long>();
    private long total = 0;

    @Override
    public String next()
    {
        Iterator<Entry<Key, Value>> si = super.getScannerIterator();
        checkState(si, true);
        while (si.hasNext())
            aggregateStats(si.next());
        return getStats();
    }

    private void aggregateStats(Entry<Key, Value> entry)
    {
        String key;
        Long count;

        key = entry.getKey().getColumnVisibility().toString();
        count = classifications.get(key);
        classifications.put(key, count != null ? count + 1 : 0L);

        key = entry.getKey().getColumnFamily().toString();
        count = columnFamilies.get(key);
        columnFamilies.put(key, count != null ? count + 1 : 0L);

        key = entry.getKey().getColumnQualifier().toString();
        count = columnQualifiers.get(key);
        columnQualifiers.put(key, count != null ? count + 1 : 0L);

        ++total;
    }

    private String getStats()
    {
        StringBuilder buf = new StringBuilder();
        buf.append("CLASSIFICATIONS:\n");
        buf.append("----------------\n");
        for (String key : classifications.keySet())
            buf.append("\t").append(key).append(": ").append(classifications.get(key)).append("\n");
        buf.append("COLUMN FAMILIES:\n");
        buf.append("----------------\n");
        for (String key : columnFamilies.keySet())
            buf.append("\t").append(key).append(": ").append(columnFamilies.get(key)).append("\n");
        buf.append("COLUMN QUALIFIERS:\n");
        buf.append("------------------\n");
        for (String key : columnQualifiers.keySet())
            buf.append("\t").append(key).append(": ").append(columnQualifiers.get(key)).append("\n");

        buf.append(total).append(" entries matched.");
        total = 0;
        classifications = new HashMap<String, Long>();
        columnFamilies = new HashMap<String, Long>();
        columnQualifiers = new HashMap<String, Long>();

        return buf.toString();
    }
}
