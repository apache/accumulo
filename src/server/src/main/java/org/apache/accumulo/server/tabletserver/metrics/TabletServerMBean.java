package org.apache.accumulo.server.tabletserver.metrics;

public interface TabletServerMBean {
	
    public int getOnlineCount();
    public int getOpeningCount();
    public int getUnopenedCount();
    public int getMajorCompactions();
    public int getMajorCompactionsQueued();
    public int getMinorCompactions();
    public int getMinorCompactionsQueued();
    public long getEntries();
    public long getEntriesInMemory();
    public long getQueries();
    public long getIngest();
    public long getTotalMinorCompactions();
    public double getHoldTime();
    public String getName();
    public double getAverageFilesPerTablet();
}
