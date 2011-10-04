package org.apache.accumulo.server.tabletserver.metrics;

public interface TabletServerUpdateMetricsMBean {

	public final static String permissionErrors = "permissionErrors";
	public final static String unknownTabletErrors = "unknownTabletErrors";
	public final static String mutationArraySize = "mutationArraysSize";
	public final static String commitPrep = "commitPrep";
	public final static String constraintViolations = "constraintViolations";
	public final static String waLogWriteTime = "waLogWriteTime";
	public final static String commitTime = "commitTime";
	
	public long getPermissionErrorCount();
	public long getUnknownTabletErrorCount();
	public long getMutationArrayAvgSize();
	public long getMutationArrayMinSize();
	public long getMutationArrayMaxSize();
	public long getCommitPrepCount();
	public long getCommitPrepMinTime();
	public long getCommitPrepMaxTime();
	public long getCommitPrepAvgTime();
	public long getConstraintViolationCount();
	public long getWALogWriteCount();
	public long getWALogWriteMinTime();
	public long getWALogWriteMaxTime();
	public long getWALogWriteAvgTime();
	public long getCommitCount();
	public long getCommitMinTime();
	public long getCommitMaxTime();
	public long getCommitAvgTime();
	public void reset();
}
