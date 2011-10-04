/**
 * 
 */
package org.apache.accumulo.server.master.state;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;


public class TableStats {
    private Map<Text, TableCounts> last = new HashMap<Text, TableCounts>();
    private Map<Text, TableCounts> next;
    private long startScan = 0;
    private long endScan = 0;

    public synchronized void begin() {
        next = new HashMap<Text, TableCounts>();
        startScan = System.currentTimeMillis();
    }

    public synchronized void update(Text tableId, TabletState state) {
        TableCounts counts = next.get(tableId);
        if (counts == null) {
            counts = new TableCounts();
            next.put(tableId, counts);
        }
        counts.counts[state.ordinal()]++;
    }

    public synchronized void end() {
        last = next;
        next = null;
        endScan = System.currentTimeMillis();
    }
    
    public synchronized Map<Text, TableCounts> getLast() {
        return last;
    }
    
    public synchronized TableCounts getLast(Text tableId) {
        TableCounts result = last.get(tableId);
        if (result == null)
            return new TableCounts();
        return result;
    }
    
    public synchronized long getScanTime() {
        if (endScan <= startScan)
            return System.currentTimeMillis() - startScan;
        return endScan - startScan;
    }
    
    public synchronized long lastScanFinished() { 
        return endScan;
    }
}