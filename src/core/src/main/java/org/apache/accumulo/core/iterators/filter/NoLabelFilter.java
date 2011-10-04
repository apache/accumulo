package org.apache.accumulo.core.iterators.filter;

import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.security.ColumnVisibility;


public class NoLabelFilter implements Filter, OptionDescriber {

		@Override
		public boolean accept(Key k, Value v) {
			ColumnVisibility label = new ColumnVisibility(k.getColumnVisibility());
			return label.getExpression().length > 0;

		}
		
		@Override
		public void init(Map<String, String> options) {
			// No Options to set
		}
		
		@Override
		public IteratorOptions describeOptions() {
			return new IteratorOptions("nolabel","NoLabelFilter hides entries without a visibility label", null, null);
		}

		@Override
		public boolean validateOptions(Map<String, String> options) { return true; }
}


