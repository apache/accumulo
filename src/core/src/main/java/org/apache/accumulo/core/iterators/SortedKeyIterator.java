package org.apache.accumulo.core.iterators;

import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;


public class SortedKeyIterator extends WrappingIterator implements OptionDescriber {
	private static final Value NOVALUE = new Value(new byte[0]);

	public SortedKeyIterator() {
	}

	public SortedKeyIterator(SortedKeyIterator other, IteratorEnvironment env) {
		setSource(other.getSource().deepCopy(env));
	}

	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
		return new SortedKeyIterator(this, env);
	}

	@Override
	public Value getTopValue() {
		return NOVALUE;
	}

	@Override
	public IteratorOptions describeOptions() {
		return new IteratorOptions("keyset", SortedKeyIterator.class.getSimpleName() + " filters out values, but leaves keys intact", null, null);
	}

	@Override
	public boolean validateOptions(Map<String, String> options) {
		return options == null || options.isEmpty();
	}
}
