package org.apache.accumulo.server.tabletserver.metrics;

import javax.management.ObjectName;

import org.apache.accumulo.server.metrics.AbstractMetricsImpl;


public class TabletServerScanMetrics extends AbstractMetricsImpl implements TabletServerScanMetricsMBean {

    static final org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(TabletServerScanMetrics.class);

    public static final String METRICS_PREFIX = "tserver.scan";
    
    public static ObjectName OBJECT_NAME = null;
    
	public TabletServerScanMetrics() {
		super();
		reset();
		try {
			OBJECT_NAME = new ObjectName("accumulo.server.metrics:service=TServerInfo,name=TabletServerScanMetricsMBean,instance="+Thread.currentThread().getName());
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

	public long getResultAvgSize() { return this.getMetricAvg(resultSize); }
	public long getResultCount() { return this.getMetricCount(resultSize); }
	public long getResultMaxSize() { return this.getMetricMax(resultSize); }
	public long getResultMinSize() { return this.getMetricMin(resultSize); }
	public long getScanAvgTime() { return this.getMetricAvg(scan); }
	public long getScanCount() { return this.getMetricCount(scan); }
	public long getScanMaxTime() { return this.getMetricMax(scan); }
	public long getScanMinTime() { return this.getMetricMin(scan); }
	public void reset() {
		createMetric(scan);
		createMetric(resultSize);
	}

}
