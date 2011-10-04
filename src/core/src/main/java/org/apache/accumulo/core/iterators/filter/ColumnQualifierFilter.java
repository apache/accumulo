package org.apache.accumulo.core.iterators.filter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;


@SuppressWarnings("deprecation")
public class ColumnQualifierFilter implements Filter {
	private boolean scanColumns;
	private HashSet<ByteSequence> columnFamilies;
	private HashMap<ByteSequence, HashSet<ByteSequence>> columnsQualifiers;
	
	public ColumnQualifierFilter(HashSet<Column> columns) {
		this.init(columns);
	}
	
	public boolean accept(Key key, Value v) {
		if(!scanColumns) 
			return true;
		
		if(columnFamilies.contains(key.getColumnFamilyData()))
			return true;
		
		HashSet<ByteSequence> cfset = columnsQualifiers.get(key.getColumnQualifierData());
		//ensure the columm qualifier goes with a paired column family,
		//it is possible that a column qualifier could occur with a 
		// column family it was not paired with
		return cfset != null &&  cfset.contains(key.getColumnFamilyData());
	}
	
    public void init(HashSet<Column> columns) {
    	this.columnFamilies = new HashSet<ByteSequence>();
		this.columnsQualifiers = new HashMap<ByteSequence, HashSet<ByteSequence>>();
		
		for(Iterator<Column> iter = columns.iterator(); iter.hasNext();) {
			Column col = iter.next();
			if(col.columnQualifier != null){
				ArrayByteSequence cq = new ArrayByteSequence(col.columnQualifier);
				HashSet<ByteSequence> cfset = this.columnsQualifiers.get(cq);
				if(cfset == null){
					cfset = new HashSet<ByteSequence>();
					this.columnsQualifiers.put(cq, cfset);
				}
				
				cfset.add(new ArrayByteSequence(col.columnFamily));
			}else{
				//this whole column family should pass
				columnFamilies.add(new ArrayByteSequence(col.columnFamily));
			}
		}		
		
		//only take action when column qualifies are present
		scanColumns = this.columnsQualifiers.size() > 0;
	}
	
	@Override
	public void init(Map<String, String> options) {
		// don't need to do anything
	}

}
