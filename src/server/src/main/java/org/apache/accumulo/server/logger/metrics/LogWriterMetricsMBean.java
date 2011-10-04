package org.apache.accumulo.server.logger.metrics;


public interface LogWriterMetricsMBean {
	
	public static final String close  = "close";
	public static final String copy = "copy";
	public static final String create = "create";
	public static final String logAppend  = "logAppend";
	public static final String logFlush = "logFlush";
	public static final String logException = "logException";


	public long getCloseCount();
	public long getCloseAvgTime();
	public long getCloseMinTime();
	public long getCloseMaxTime();
	public long getCopyCount();
	public long getCopyAvgTime();
	public long getCopyMinTime();
	public long getCopyMaxTime();
	public long getCreateCount();
	public long getCreateMinTime();
	public long getCreateMaxTime();
	public long getCreateAvgTime();
	public long getLogAppendCount();
	public long getLogAppendMinTime();
	public long getLogAppendMaxTime();
	public long getLogAppendAvgTime();
	public long getLogFlushCount();
	public long getLogFlushMinTime();
	public long getLogFlushMaxTime();
	public long getLogFlushAvgTime();
	public long getLogExceptionCount();
	public void reset();
	
}
