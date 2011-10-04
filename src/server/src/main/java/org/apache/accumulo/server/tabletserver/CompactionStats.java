package org.apache.accumulo.server.tabletserver;

public class CompactionStats {
	private long entriesRead;
	private long entriesWritten;
	private long fileSize;

	CompactionStats(long er, long ew){
		this.setEntriesRead(er);
		this.setEntriesWritten(ew);
	}

	public CompactionStats() {
	}

	private void setEntriesRead(long entriesRead) {
		this.entriesRead = entriesRead;
	}

	public long getEntriesRead() {
		return entriesRead;
	}

	private void setEntriesWritten(long entriesWritten) {
		this.entriesWritten = entriesWritten;
	}

	public long getEntriesWritten() {
		return entriesWritten;
	}

	public void add(CompactionStats mcs) {
		this.entriesRead += mcs.entriesRead;
		this.entriesWritten += mcs.entriesWritten;
	}

	public void setFileSize(long fileSize) {
		this.fileSize = fileSize;
	}
	
	public long getFileSize(){
		return this.fileSize;
	}
}
