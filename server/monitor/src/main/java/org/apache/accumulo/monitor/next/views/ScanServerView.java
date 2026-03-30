package org.apache.accumulo.monitor.next.views;

import static org.apache.accumulo.core.metrics.Metric.SCAN_OPEN_FILES;
import static org.apache.accumulo.core.metrics.Metric.SCAN_QUERIES;
import static org.apache.accumulo.core.metrics.Metric.SCAN_SCANNED_ENTRIES;
import static org.apache.accumulo.core.metrics.Metric.SCAN_QUERY_SCAN_RESULTS;
import static org.apache.accumulo.core.metrics.Metric.SCAN_QUERY_SCAN_RESULTS_BYTES;
import static org.apache.accumulo.core.metrics.Metric.SCAN_BUSY_TIMEOUT_COUNT;
import static org.apache.accumulo.core.metrics.Metric.SCAN_RESERVATION_CONFLICT_COUNTER;
import static org.apache.accumulo.core.metrics.Metric.SCAN_ZOMBIE_THREADS;
import static org.apache.accumulo.core.metrics.Metric.SERVER_IDLE;
import static org.apache.accumulo.core.metrics.Metric.LOW_MEMORY;
import static org.apache.accumulo.core.metrics.Metric.SCAN_PAUSED_FOR_MEM;
import static org.apache.accumulo.core.metrics.Metric.SCAN_RETURN_FOR_MEM;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.process.thrift.MetricResponse;

public class ScanServerView extends ServerView {

  private static final Map<String, String> mapping;
  
  static {
    Map<String, String> m = new HashMap<>();
    m.put(SCAN_OPEN_FILES.getName(), "openFiles");
    m.put(SCAN_QUERIES.getName(), "queries");
    m.put(SCAN_SCANNED_ENTRIES.getName(), "scannedEntries");
    m.put(SCAN_QUERY_SCAN_RESULTS.getName(), "queryResults");
    m.put(SCAN_QUERY_SCAN_RESULTS_BYTES.getName(), "queryResultBytes");
    m.put(SCAN_BUSY_TIMEOUT_COUNT.getName(), "busyTimeouts");
    m.put(SCAN_RESERVATION_CONFLICT_COUNTER.getName(), "reservationConflicts");
    m.put(SCAN_ZOMBIE_THREADS.getName(), "zombieThreads");
    m.put(SERVER_IDLE.getName(), "serverIdle");
    m.put(LOW_MEMORY.getName(), "lowMemory");
    m.put(SCAN_PAUSED_FOR_MEM.getName(), "scansPausedForMemory");
    m.put(SCAN_RETURN_FOR_MEM.getName(), "scansReturnedEarlyForMemory");
    mapping = Map.copyOf(m);
  }
  
  protected ScanServerView(MetricResponse metrics) {
    super(metrics, mapping);
  }

}
