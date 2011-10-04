package org.apache.accumulo.server.constraints;

import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.constraints.Constraint;
import org.apache.accumulo.core.data.Mutation;


public class UnsatisfiableConstraint implements Constraint {

	private List<Short> violations;
	private String vDesc;
	
	public UnsatisfiableConstraint(short vcode, String violationDescription){
		this.violations = Collections.unmodifiableList(Collections.singletonList(vcode));
		this.vDesc = violationDescription;
	}
	
	public List<Short> check(Mutation mutation) {
		return violations;
	}

	public String getViolationDescription(short violationCode) {
		return vDesc;
	}

}
