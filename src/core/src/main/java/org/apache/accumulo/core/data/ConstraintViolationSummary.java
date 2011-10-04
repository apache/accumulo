package org.apache.accumulo.core.data;

import org.apache.accumulo.core.data.thrift.TConstraintViolationSummary;

public class ConstraintViolationSummary {

  public String constrainClass;
  public short violationCode;
  public String violationDescription;
  public long numberOfViolatingMutations;

 
  public ConstraintViolationSummary(
    String constrainClass,
    short violationCode,
    String violationDescription,
    long numberOfViolatingMutations)
  {
    this.constrainClass = constrainClass;
    this.violationCode = violationCode;
    this.violationDescription = violationDescription;
    this.numberOfViolatingMutations = numberOfViolatingMutations;
  }

  public ConstraintViolationSummary(TConstraintViolationSummary tcvs){
	  this(tcvs.constrainClass, tcvs.violationCode, tcvs.violationDescription, tcvs.numberOfViolatingMutations);
  }
  

  public String getConstrainClass() {
    return this.constrainClass;
  }

  public short getViolationCode() {
    return this.violationCode;
  }

  public String getViolationDescription() {
    return this.violationDescription;
  }

  public long getNumberOfViolatingMutations() {
    return this.numberOfViolatingMutations;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ConstraintViolationSummary(");
    boolean first = true;

    sb.append("constrainClass:");
    if (this.constrainClass == null) {
      sb.append("null");
    } else {
      sb.append(this.constrainClass);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("violationCode:");
    sb.append(this.violationCode);
    first = false;
    if (!first) sb.append(", ");
    sb.append("violationDescription:");
    if (this.violationDescription == null) {
      sb.append("null");
    } else {
      sb.append(this.violationDescription);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("numberOfViolatingMutations:");
    sb.append(this.numberOfViolatingMutations);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public TConstraintViolationSummary toThrift() {
	  return new TConstraintViolationSummary(this.constrainClass, violationCode, violationDescription, numberOfViolatingMutations);
  }

}

