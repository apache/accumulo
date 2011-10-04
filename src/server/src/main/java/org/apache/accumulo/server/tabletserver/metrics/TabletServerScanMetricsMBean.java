package org.apache.accumulo.server.tabletserver.metrics;

public interface TabletServerScanMetricsMBean {
	
	public static final String scan = "scan";
	public static final String resultSize = "result";
	
	public long getScanCount();
	public long getScanAvgTime();
	public long getScanMinTime();
	public long getScanMaxTime();
	public long getResultCount();
	public long getResultAvgSize();
	public long getResultMinSize();
	public long getResultMaxSize();
	public void reset();

}
