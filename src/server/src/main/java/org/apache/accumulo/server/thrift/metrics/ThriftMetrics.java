package org.apache.accumulo.server.thrift.metrics;

import javax.management.ObjectName;

import org.apache.accumulo.server.metrics.AbstractMetricsImpl;

public class ThriftMetrics extends AbstractMetricsImpl implements ThriftMetricsMBean {
  
  static final org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(ThriftMetrics.class);
  
  private static final String METRICS_PREFIX = "thrift";
  
  private static ObjectName OBJECT_NAME = null;
  
  public ThriftMetrics(String serverName, String threadName) {
    super();
    reset();
    try {
      OBJECT_NAME = new ObjectName("accumulo.server.metrics:service=" + serverName + ",name=ThriftMetricsMBean,instance=" + threadName);
    } catch (Exception e) {
      log.error("Exception setting MBean object name", e);
    }
  }
  
  @Override
  protected ObjectName getObjectName() {
    return OBJECT_NAME;
  }
  
  @Override
  protected String getMetricsPrefix() {
    return METRICS_PREFIX;
  }
  
  public void reset() {
    createMetric(idle);
    createMetric(execute);
  }
  
  public long getExecutionAvgTime() {
    return this.getMetricAvg(execute);
  }
  
  public long getExecutionCount() {
    return this.getMetricCount(execute);
  }
  
  public long getExecutionMaxTime() {
    return this.getMetricMax(execute);
  }
  
  public long getExecutionMinTime() {
    return this.getMetricMin(execute);
  }
  
  public long getIdleAvgTime() {
    return this.getMetricAvg(idle);
  }
  
  public long getIdleCount() {
    return this.getMetricCount(idle);
  }
  
  public long getIdleMaxTime() {
    return this.getMetricMax(idle);
  }
  
  public long getIdleMinTime() {
    return this.getMetricMin(idle);
  }
  
}
