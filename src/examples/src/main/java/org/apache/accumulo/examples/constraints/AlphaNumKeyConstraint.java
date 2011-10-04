package org.apache.accumulo.examples.constraints;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.accumulo.core.constraints.Constraint;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;


/**
 * This class is a accumulo constraint that ensures all 
 * fields of a key are alpha numeric.
 * 
 * 
 *
 */

public class AlphaNumKeyConstraint implements Constraint {

	private static final short NON_ALPHA_NUM_ROW = 1;
	private static final short NON_ALPHA_NUM_COLF = 2;
	private static final short NON_ALPHA_NUM_COLQ = 3;
	
	private boolean isAlphaNum(byte bytes[]){
		for (byte b : bytes) {
			boolean ok = ((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9'));
			if(!ok) return false;
		}
		
		return true;
	}
	
	private List<Short> addViolation(List<Short> violations, short violation) {
		if(violations == null){
			violations = new ArrayList<Short>();
			violations.add(violation);
		}else if(!violations.contains(violation)){
			violations.add(violation);
		}
		return violations;
	}
	
	@Override
	public List<Short> check(Environment env, Mutation mutation) {
		List<Short> violations = null;
		
		if(!isAlphaNum(mutation.getRow()))
			violations = addViolation(violations, NON_ALPHA_NUM_ROW);
		
		Collection<ColumnUpdate> updates = mutation.getUpdates();
		for (ColumnUpdate columnUpdate : updates) {
			if(!isAlphaNum(columnUpdate.getColumnFamily()))
				violations = addViolation(violations, NON_ALPHA_NUM_COLF);
			
			if(!isAlphaNum(columnUpdate.getColumnQualifier()))
				violations = addViolation(violations, NON_ALPHA_NUM_COLQ);
		}
		
		return violations;
	}

	@Override
	public String getViolationDescription(short violationCode) {
		
		switch(violationCode){
		case NON_ALPHA_NUM_ROW:
			return "Row was not alpha numeric";
		case NON_ALPHA_NUM_COLF:
			return "Column family was not alpha numeric";
		case NON_ALPHA_NUM_COLQ:
			return "Column qualifier was not alpha numeric";
		}
		
		return null;
	}

}
