package org.apache.accumulo.server.tabletserver.metrics;

import javax.management.ObjectName;

import org.apache.accumulo.server.metrics.AbstractMetricsImpl;


public class TabletServerUpdateMetrics extends AbstractMetricsImpl implements TabletServerUpdateMetricsMBean {

    static final org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(TabletServerUpdateMetrics.class);
	
    private static final String METRICS_PREFIX = "tserver.update";
    
    private static ObjectName OBJECT_NAME = null;

	public TabletServerUpdateMetrics() {
		super();
		reset();
		try {
			OBJECT_NAME = new ObjectName("accumulo.server.metrics:service=TServerInfo,name=TabletServerUpdateMetricsMBean,instance="+Thread.currentThread().getName());
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

	public long getPermissionErrorCount() { return this.getMetricCount(permissionErrors); }
	public long getUnknownTabletErrorCount() { return this.getMetricCount(unknownTabletErrors); }
	public long getMutationArrayAvgSize() { return this.getMetricAvg(mutationArraySize); }
	public long getMutationArrayMinSize() { return this.getMetricMin(mutationArraySize); }
	public long getMutationArrayMaxSize() { return this.getMetricMax(mutationArraySize); }
	public long getCommitPrepCount() { return this.getMetricCount(commitPrep); }
	public long getCommitPrepMinTime() { return this.getMetricMin(commitPrep); }
	public long getCommitPrepMaxTime() { return this.getMetricMax(commitPrep); }
	public long getCommitPrepAvgTime() { return this.getMetricAvg(commitPrep); }
	public long getConstraintViolationCount() { return this.getMetricCount(constraintViolations); }
	public long getWALogWriteCount() { return this.getMetricCount(waLogWriteTime); }
	public long getWALogWriteMinTime() { return this.getMetricMin(waLogWriteTime); }
	public long getWALogWriteMaxTime() { return this.getMetricMax(waLogWriteTime); }
	public long getWALogWriteAvgTime() { return this.getMetricAvg(waLogWriteTime); }
	public long getCommitCount() { return this.getMetricCount(commitTime); }
	public long getCommitMinTime() { return this.getMetricMin(commitTime); }
	public long getCommitMaxTime() { return this.getMetricMax(commitTime); }
	public long getCommitAvgTime() { return this.getMetricAvg(commitTime); }
	public void reset() {
		createMetric(permissionErrors);
		createMetric(unknownTabletErrors);
		createMetric(mutationArraySize);
		createMetric(commitPrep);
		createMetric(constraintViolations);
		createMetric(waLogWriteTime);
		createMetric(commitTime);
	}

}
