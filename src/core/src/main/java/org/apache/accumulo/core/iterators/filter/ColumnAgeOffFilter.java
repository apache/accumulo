package org.apache.accumulo.core.iterators.filter;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.conf.ColumnToClassMapping;
import org.apache.accumulo.core.iterators.conf.PerColumnIteratorConfig;


public class ColumnAgeOffFilter implements Filter, OptionDescriber {
	
	private class TTLSet extends ColumnToClassMapping<Long> {
		public TTLSet(Map<String, String> objectStrings) {
			super();
			
			for (Entry<String, String> entry : objectStrings.entrySet()) {
				String column = entry.getKey();
				String ttl = entry.getValue();
				Long l = Long.parseLong(ttl);
				
				PerColumnIteratorConfig ac = PerColumnIteratorConfig.decodeColumns(column, ttl);
				
				if(ac.getColumnQualifier() == null){
					addObject(ac.getColumnFamily(), l);
				}else{
					addObject(ac.getColumnFamily(), ac.getColumnQualifier(), l);
				}
			}
		}
	}
	
	TTLSet ttls;
	long currentTime = 0;

	@Override
	public boolean accept(Key k, Value v) {
		Long threshold = ttls.getObject(k);
		if (threshold == null)
			return true;
		if (currentTime - k.getTimestamp() > threshold)
			return false;
		return true;
	}

	@Override
	public void init(Map<String, String> options) {
		this.ttls = new TTLSet(options);
		currentTime = System.currentTimeMillis();
	}
	
	public void overrideCurrentTime(long ts) {
		this.currentTime = ts;
	}

	@Override
	public IteratorOptions describeOptions() {
		return new IteratorOptions("colageoff","time to live in milliseconds for each column",
				null, Collections.singletonList("<columnName> <Long>"));
	}

	@Override
	public boolean validateOptions(Map<String, String> options) {
		this.ttls = new TTLSet(options);
		return true;
	}

}
