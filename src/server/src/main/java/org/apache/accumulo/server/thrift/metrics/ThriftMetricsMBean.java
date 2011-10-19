package org.apache.accumulo.server.thrift.metrics;

public interface ThriftMetricsMBean {
    
    public static final String idle = "idle";
    public static final String execute = "execute";
    
    public long getIdleCount();
    
    public long getIdleMinTime();
    
    public long getIdleMaxTime();
    
    public long getIdleAvgTime();
    
    public long getExecutionCount();
    
    public long getExecutionMinTime();
    
    public long getExecutionMaxTime();
    
    public long getExecutionAvgTime();
    
    public void reset();
    
}
