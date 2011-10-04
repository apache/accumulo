package org.apache.accumulo.core.iterators.system;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;


public class TimeSettingIterator implements InterruptibleIterator {

	private SortedKeyValueIterator<Key, Value> source;
	private long time;
	private Range range;

	public TimeSettingIterator(SortedKeyValueIterator<Key, Value> source, long time) {
		this.source =source;
		this.time = time;
	}

	@Override
	public Key getTopKey() {
		Key key = new Key(source.getTopKey());
		key.setTimestamp(time);
		return key;
	}
	
	@Override
	public void setInterruptFlag(AtomicBoolean flag) {
		((InterruptibleIterator)source).setInterruptFlag(flag);
	}

	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
		return new TimeSettingIterator(source.deepCopy(env), time);
	}

	@Override
	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
		
	}

	@Override
	public boolean hasTop() {
		return source.hasTop() && !range.afterEndKey(getTopKey());
	}

	@Override
	public void next() throws IOException {
		source.next();
	}

	@Override
	public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
		Range seekRange = IteratorUtil.maximizeStartKeyTimeStamp(range);
		seekRange = IteratorUtil.minimizeEndKeyTimeStamp(seekRange);
		source.seek(seekRange, columnFamilies, inclusive);
		this.range = range;
		while(hasTop() && range.beforeStartKey(getTopKey())){
			next();
		}
		
	}

	@Override
	public Value getTopValue() {
		return source.getTopValue();
	}

}
