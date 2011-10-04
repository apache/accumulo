package org.apache.accumulo.core.iterators;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;


public class GrepIterator extends SkippingIterator {

	private byte term[];
	
	@Override
	protected void consume() throws IOException {
		while(getSource().hasTop()){
			Key k = getSource().getTopKey();
			Value v = getSource().getTopValue();
			
			if(match(v.get()) || match(k.getRowData()) || match(k.getColumnFamilyData()) || match(k.getColumnQualifierData())){
				break;
			}
			
			getSource().next();
		}
	}

	private boolean match(ByteSequence bs) {
		return indexOf(bs.getBackingArray(), bs.offset(), bs.length(), term) >= 0;
	}

	private boolean match(byte[] ba) {
		return indexOf(ba, 0, ba.length, term) >= 0;
	}

	//copied code below from java string and modified
	
	private static int indexOf(byte[] source, int sourceOffset, int sourceCount, byte[] target) {
		byte first  = target[0];
		int targetCount = target.length;
		int max = sourceOffset + (sourceCount - targetCount);

		for (int i = sourceOffset; i <= max; i++) {
			/* Look for first character. */
			if (source[i] != first) {
				while (++i <= max && source[i] != first)
					continue;
			}

			/* Found first character, now look at the rest of v2 */
			if (i <= max) {
				int j = i + 1;
				int end = j + targetCount - 1;
				for (int k = 1; j < end && source[j] ==
					target[k]; j++, k++)
					continue;

				if (j == end) {
					/* Found whole string. */
					return i - sourceOffset;
				}
			}
		}
		return -1;
	}

	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
		super.init(source, options, env);
		term = options.get("term").getBytes();
	}
	
}
