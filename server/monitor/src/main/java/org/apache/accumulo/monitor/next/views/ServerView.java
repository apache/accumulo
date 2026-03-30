package org.apache.accumulo.monitor.next.views;

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.accumulo.core.process.thrift.MetricResponse;
import org.apache.accumulo.monitor.next.sservers.ScanServerView;

public abstract class ServerView {

  public String type;
  public String resourceGroup;
  public String address;
  public long timestamp;
  public Map<String,Number> metrics;
  
  /**
   * @param metrics MetricResponse from the server
   * @param metricMapping mapping of internal metric name to external name
   */
  protected ServerView(MetricResponse metrics, Map<String,String> metricMapping) {
    this.type = metrics.serverType.name();
    this.resourceGroup = metrics.getResourceGroup();
    this.address = metrics.getServer();
    this.timestamp = metrics.getTimestamp();
    Map<String, Number> allMetrics = ScanServerView.metricValuesByName(metrics);
    Map<String, Number> convertedMetrics = new TreeMap<>();
    for (Entry<String, Number> e : allMetrics.entrySet()) {
      String n = metricMapping.get(e.getKey());
      if (n != null) {
        convertedMetrics.put(n, e.getValue());
      }
    }
    this.metrics = Map.copyOf(convertedMetrics);
  }
}
