package ingest;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.StatusReporter;

public class StandaloneStatusReporter extends StatusReporter {
	
	private Counters c = new Counters();
	
	private long filesProcessed = 0;
	private long recordsProcessed = 0;

	public Counters getCounters() {
		return c;
	}

	@Override
	public Counter getCounter(Enum<?> name) {
		return c.findCounter(name);
	}

	@Override
	public Counter getCounter(String group, String name) {
		return c.findCounter(group, name);
	}

	@Override
	public void progress() {
		// do nothing
	}

	@Override
	public void setStatus(String status) {
		// do nothing
	}

	public long getFilesProcessed() {
		return filesProcessed;
	}
	
	public long getRecordsProcessed() {
		return recordsProcessed;
	}
	
	public void incrementFilesProcessed() {
		filesProcessed++;
		recordsProcessed = 0;
	}
	
	public void incrementRecordsProcessed() {
		recordsProcessed++;
	}
}
