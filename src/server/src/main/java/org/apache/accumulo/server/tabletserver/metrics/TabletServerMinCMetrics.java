package org.apache.accumulo.server.tabletserver.metrics;

import javax.management.ObjectName;

import org.apache.accumulo.server.metrics.AbstractMetricsImpl;


public class TabletServerMinCMetrics extends AbstractMetricsImpl implements
		TabletServerMinCMetricsMBean {

    static final org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(TabletServerMinCMetrics.class);
	
    private static final String METRICS_PREFIX = "tserver.minc";
    
    private static ObjectName OBJECT_NAME = null;
    
	public TabletServerMinCMetrics() {
		super();
		reset();
		try {
			OBJECT_NAME = new ObjectName("accumulo.server.metrics:service=TServerInfo,name=TabletServerMinCMetricsMBean,instance="+Thread.currentThread().getName());
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

	public long getMinorCompactionMinTime() { return this.getMetricMin(minc); }
	public long getMinorCompactionAvgTime() { return this.getMetricAvg(minc); }
	public long getMinorCompactionCount() { return this.getMetricCount(minc); }
	public long getMinorCompactionMaxTime() { return this.getMetricMax(minc); }
	public long getMinorCompactionQueueAvgTime() { return this.getMetricAvg(queue); }
	public long getMinorCompactionQueueCount() { return this.getMetricCount(queue); }
	public long getMinorCompactionQueueMaxTime() { return this.getMetricMax(queue); }
	public long getMinorCompactionQueueMinTime() { return this.getMetricMin(minc); }
	public void reset() {
		createMetric("minc");
		createMetric("queue");
	}

}
