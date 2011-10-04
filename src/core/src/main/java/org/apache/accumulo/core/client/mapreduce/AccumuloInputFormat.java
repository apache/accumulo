package org.apache.accumulo.core.client.mapreduce;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


/**
 * This class allows MapReduce jobs to use Accumulo as the source of data. This
 * input format provides keys and values of type Key and Value to the Map() and
 * Reduce() functions.
 * 
 * The user must specify the following via static methods:
 * 
 * <ul>
 * <li>AccumuloInputFormat.setInputTableInfo(job, username, password, table,
 * auths)
 * <li>AccumuloInputFormat.setZooKeeperInstance(job, instanceName, hosts)
 * </ul>
 * 
 * Other static methods are optional
 */

public class AccumuloInputFormat extends InputFormatBase<Key,Value> {
	@Override
	public RecordReader<Key, Value> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		log.setLevel(getLogLevel(context));
		return new RecordReaderBase<Key,Value>(){
			@Override
			public boolean nextKeyValue() throws IOException, InterruptedException {
				if (scannerIterator.hasNext()) {
					++numKeysRead;
					Entry<Key, Value> entry = scannerIterator.next();
					currentK = currentKey = entry.getKey();
					currentV = currentValue = entry.getValue();
					if (log.isTraceEnabled())
						log.trace("Processing key/value pair: " + DefaultFormatter.formatEntry(entry, true));
					return true;
				}
				return false;
			}		
		};
	}
}