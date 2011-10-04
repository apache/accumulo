package org.apache.accumulo.core.client.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class AccumuloRowInputFormat extends InputFormatBase<Text, List<Entry<Key, Value>>> {
	@Override
	public RecordReader<Text, List<Entry<Key, Value>>> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new RecordReaderBase<Text, List<Entry<Key, Value>>>() {
			RowIterator rowIterator;
			
			@Override
			public void initialize(InputSplit inSplit,
					TaskAttemptContext attempt) throws IOException {
				super.initialize(inSplit, attempt);
				rowIterator = new RowIterator(scannerIterator);
				currentK = new Text();
				currentV = new ArrayList<Entry<Key,Value>>();
			}
			
			@Override
			public boolean nextKeyValue() throws IOException,
					InterruptedException {
				if (!rowIterator.hasNext()) return false;
				currentV = rowIterator.next();
				numKeysRead += currentV.size();
				currentK = new Text(currentV.get(0).getKey().getRow());
				currentKey = currentV.get(currentV.size()-1).getKey();
				return true;
			}			
		};
	}
}
