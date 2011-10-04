package org.apache.accumulo.core.iterators;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.filter.RegExFilter;


public class RegExIterator extends SkippingIterator implements OptionDescriber {
	
	private RegExFilter ref = new RegExFilter();

	public RegExIterator deepCopy(IteratorEnvironment env)
	{
		return new RegExIterator(this, env);
	}
	
	private RegExIterator(RegExIterator other, IteratorEnvironment env)
	{
		setSource(other.getSource().deepCopy(env));
		ref = other.ref;
	}
	
	public RegExIterator()
	{
		
	}
	
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
		consume();
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
