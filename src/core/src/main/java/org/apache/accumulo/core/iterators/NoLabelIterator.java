package org.apache.accumulo.core.iterators;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.filter.NoLabelFilter;


public class NoLabelIterator extends SkippingIterator implements OptionDescriber {
	
	private NoLabelFilter ref = new NoLabelFilter();
	
	public NoLabelIterator deepCopy(IteratorEnvironment env)
	{
		return new NoLabelIterator(this, env);
	}
	
	private NoLabelIterator(NoLabelIterator other, IteratorEnvironment env)
	{
		setSource(other.getSource().deepCopy(env));
		ref = other.ref;
	}
	
	public NoLabelIterator(){}
	
	private boolean matches(Key key, Value value){
		return ref.accept(key, value);			
	}
	
	@Override
	protected void consume() throws IOException {
		while(getSource().hasTop() && !matches(getSource().getTopKey(), getSource().getTopValue())){
			getSource().next();
		}
	}
	
	
	
	@Override
	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
		super.init(source, options, env);
		ref.init(options);		
	}
	
	@Override
	public IteratorOptions describeOptions() {
		return ref.describeOptions();
	}
	
	@Override
	public boolean validateOptions(Map<String, String> options) {
		return ref.validateOptions(options);
	}	
}
