package org.apache.accumulo.server.tabletserver.metrics;

public interface TabletServerMinCMetricsMBean {
	
	public static final String minc = "minc";
	public static final String queue = "queue";
	
	public long getMinorCompactionCount();
	public long getMinorCompactionAvgTime();
	public long getMinorCompactionMinTime();
	public long getMinorCompactionMaxTime();
	public long getMinorCompactionQueueCount();
	public long getMinorCompactionQueueAvgTime();
	public long getMinorCompactionQueueMinTime();
	public long getMinorCompactionQueueMaxTime();
	public void reset();

}
