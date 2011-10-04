package org.apache.accumulo.core.client.mapreduce;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.junit.Before;
import org.junit.Test;

public class AccumuloFileOutputFormatTest {
    JobContext job;
    TaskAttemptContext tac;

	@Before
	public void setup() {
		job = new JobContext(new Configuration(), new JobID());
		tac = new TaskAttemptContext(job.getConfiguration(), new TaskAttemptID());
	}
	
	@Test
	public void testSet() throws IOException, InterruptedException {
        AccumuloFileOutputFormat.setBlockSize(job, 300);
        validate(300);
	}
	
	@Test
	public void testUnset() throws IOException, InterruptedException {
		validate((int) AccumuloConfiguration.getDefaultConfiguration().getMemoryInBytes(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE));
	}
	
	public void validate(int size) throws IOException, InterruptedException
	{
		AccumuloFileOutputFormat.handleBlockSize(job);
		int detSize = job.getConfiguration().getInt("io.seqfile.compress.blocksize", -1);
		assertEquals(size, detSize);
	}

}
