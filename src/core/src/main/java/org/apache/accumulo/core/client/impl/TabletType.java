package org.apache.accumulo.core.client.impl;

import java.util.Collection;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.KeyExtent;



public enum TabletType {
	ROOT,
	METADATA,
	USER;
	
	public static TabletType type(KeyExtent ke){
		if(ke.equals(Constants.ROOT_TABLET_EXTENT))
			return ROOT;
		if(ke.getTableId().toString().equals(Constants.METADATA_TABLE_ID))
			return METADATA;
		return USER;
	}
	
	public static TabletType type(Collection<KeyExtent> extents){
		if(extents.size() == 0)
			throw new IllegalArgumentException();
		
		TabletType ttype = null;
		
		for(KeyExtent extent : extents){
			if(ttype == null)
				ttype = type(extent);
			else if(ttype != type(extent))
				throw new IllegalArgumentException("multiple extent types not allowed "+ttype+" "+type(extent));
		}
		
		return ttype;
	}
}
