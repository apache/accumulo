package org.apache.accumulo.core.client.mapreduce;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class AccumuloRowInputFormat extends InputFormatBase<Text, PeekingIterator<Entry<Key, Value>>> {
	@Override
	public RecordReader<Text, PeekingIterator<Entry<Key, Value>>> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new RecordReaderBase<Text, PeekingIterator<Entry<Key, Value>>>() {
			RowIterator rowIterator;
			
			@Override
			public void initialize(InputSplit inSplit,
					TaskAttemptContext attempt) throws IOException {
				super.initialize(inSplit, attempt);
				rowIterator = new RowIterator(scannerIterator);
				currentK = new Text();
				currentV = null;
			}
			
			@Override
			public boolean nextKeyValue() throws IOException,
					InterruptedException {
				if (!rowIterator.hasNext()) return false;
				currentV = new PeekingIterator<Entry<Key, Value>>(rowIterator.next());
				numKeysRead = rowIterator.getKVCount();
				currentKey = currentV.peek().getKey();
				currentK = new Text(currentKey.getRow());
				return true;
			}			
		};
	}
}
