package org.apache.accumulo.core.iterators.conf;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.iterators.conf.ColumnUtil.ColFamHashKey;
import org.apache.accumulo.core.iterators.conf.ColumnUtil.ColHashKey;
import org.apache.hadoop.io.Text;


@SuppressWarnings("deprecation")
public class ColumnSet {
	private Set<ColFamHashKey> objectsCF;
	private Set<ColHashKey> objectsCol;
	
	private ColHashKey lookupCol = new ColHashKey();
	private ColFamHashKey lookupCF = new ColFamHashKey();
	
	public ColumnSet(){
		objectsCF = new HashSet<ColFamHashKey>();
		objectsCol = new HashSet<ColHashKey>();
	}
	
	public ColumnSet(Collection<String> objectStrings) {
		this();
		
		for (String column : objectStrings) {
			PerColumnIteratorConfig pcic = PerColumnIteratorConfig.decodeColumns(column, null);
			
			if(pcic.getColumnQualifier() == null){
				add(pcic.getColumnFamily());
			}else{
				add(pcic.getColumnFamily(), pcic.getColumnQualifier());
			}
		}
	}

	protected void add(Text colf){
		objectsCF.add(new ColFamHashKey(new Text(colf)));
	}
	
	protected void add(Text colf, Text colq){
		objectsCol.add(new ColHashKey(colf, colq));
	}
	
	public boolean contains(Key key) {
		//lookup column family and column qualifier
		if(objectsCol.size() > 0){
			lookupCol.set(key);
			if (objectsCol.contains(lookupCol)) return true;
		}
		
		//lookup just column family
		if(objectsCF.size() > 0){
			lookupCF.set(key);
			return objectsCF.contains(lookupCF);
		}
		
		return false;
	}
	
	public boolean isEmpty() {
		return objectsCol.size() == 0 && objectsCF.size() == 0;
	}
}
