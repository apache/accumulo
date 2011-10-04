package org.apache.accumulo.core.constraints;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.ConstraintViolationSummary;


public class Violations {
	
	private static class CVSKey {
		private String className;
		private short vcode;
		
		CVSKey(ConstraintViolationSummary cvs){
			this.className = cvs.constrainClass;
			this.vcode = cvs.violationCode;
		}
		
		
		@Override
		public int hashCode(){
			return className.hashCode() + vcode;
		}
		
		@Override
		public boolean equals(Object o){
			if (o instanceof CVSKey)
				return equals((CVSKey) o);
			return false;
		}
		
		public boolean equals(CVSKey ocvsk) {
			return className.equals(ocvsk.className) && vcode == ocvsk.vcode;
		}
	}
	
	private HashMap<CVSKey, ConstraintViolationSummary> cvsmap;
	
	public Violations(){
		cvsmap = new HashMap<CVSKey, ConstraintViolationSummary>();
	}
	
	public boolean isEmpty() {
	    return cvsmap.isEmpty();
	}
	
	private void add(CVSKey cvsk, ConstraintViolationSummary cvs){
		ConstraintViolationSummary existingCvs = cvsmap.get(cvsk);
		
		if(existingCvs == null){
			cvsmap.put(cvsk, cvs);
		}else{
			existingCvs.numberOfViolatingMutations += cvs.numberOfViolatingMutations;
		}
	}
	
	public void add(ConstraintViolationSummary cvs) {
		CVSKey cvsk = new CVSKey(cvs);
		add(cvsk, cvs);
	}

	public void add(Violations violations) {
		Set<Entry<CVSKey, ConstraintViolationSummary>> es = violations.cvsmap.entrySet();
		
		for (Entry<CVSKey, ConstraintViolationSummary> entry : es) {
			add(entry.getKey(), entry.getValue());
		}
		
	}

	public void add(List<ConstraintViolationSummary> cvsList) {
		for (ConstraintViolationSummary constraintViolationSummary : cvsList) {
			add(constraintViolationSummary);
		}
		
	}
	
	public List<ConstraintViolationSummary> asList() {
		return new ArrayList<ConstraintViolationSummary>(cvsmap.values());
	}
	
}
