package org.apache.accumulo.core.iterators.filter;

import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.VisibilityEvaluator;
import org.apache.accumulo.core.security.VisibilityParseException;
import org.apache.commons.collections.map.LRUMap;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;


@SuppressWarnings("deprecation")
public class VisibilityFilter implements Filter {
	private VisibilityEvaluator ve;
	private Text defaultVisibility;
	private LRUMap cache;
	private Text tmpVis;
	
	private static final Logger log = Logger.getLogger(VisibilityFilter.class);
	
	public VisibilityFilter(Authorizations authorizations, byte[] defaultVisibility) {
	    this.ve = new VisibilityEvaluator(authorizations);
        this.defaultVisibility = new Text(defaultVisibility);
        this.cache = new LRUMap(1000);
        this.tmpVis = new Text();
	}
	
	public boolean accept(Key k, Value v) {
		Text testVis = k.getColumnVisibility(tmpVis);
		
		if (testVis.getLength() == 0 && defaultVisibility.getLength() == 0)
			return true;
		else if (testVis.getLength() == 0)
			testVis = defaultVisibility;
		
		Boolean b = (Boolean) cache.get(testVis);
		if (b != null)
			return b;
		
		try {
			Boolean bb = ve.evaluate(new ColumnVisibility(testVis));
			cache.put(new Text(testVis), bb);
			return bb;
		} catch (VisibilityParseException e) {
			log.error("Parse Error", e);
			return false;
		}
	}
	
	@Override
	public void init(Map<String, String> options) {
	}
}
