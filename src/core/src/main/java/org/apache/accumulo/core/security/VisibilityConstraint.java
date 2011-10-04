package org.apache.accumulo.core.security;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.accumulo.core.constraints.Constraint;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.util.BadArgumentException;


public class VisibilityConstraint implements Constraint {

	@Override
	public String getViolationDescription(short violationCode) {
		switch(violationCode){
		case 1:
			return "Malformed column visibility";
		case 2:
			return "User does not have authorization on column visibility";
		}
		
		return null;
	}
	
	@Override
	public List<Short> check(Environment env, Mutation mutation) {
		List<ColumnUpdate> updates = mutation.getUpdates();
		
		HashSet<String> ok = null;
		if(updates.size() > 1)
			ok = new HashSet<String>();
		
		VisibilityEvaluator ve = null;
		
		for (ColumnUpdate update : updates) {
			
			byte[] cv = update.getColumnVisibility();
			if(cv.length > 0){
				String key = null;
				if(ok != null && ok.contains(key = new String(cv)))
					continue;
				
				try{
					
					if(ve == null)
						ve = new VisibilityEvaluator(env.getAuthorizations());
					
					if(!ve.evaluate(new ColumnVisibility(cv)))
						return Collections.singletonList(new Short((short)2));
					
				}catch(BadArgumentException bae){
					return Collections.singletonList(new Short((short)1));
				} catch (VisibilityParseException e) {
					return Collections.singletonList(new Short((short)1));
				}
				
				if(ok!= null)
					ok.add(key);
			}
		}
		
		return null;
	}
}
