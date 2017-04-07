/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.data;

import org.apache.accumulo.core.data.thrift.TConstraintViolationSummary;

/**
 * A summary of constraint violations across some number of mutations.
 */
public class ConstraintViolationSummary {

  public String constrainClass;
  public short violationCode;
  public String violationDescription;
  public long numberOfViolatingMutations;

  /**
   * Creates a new summary.
   *
   * @param constrainClass
   *          class of constraint that was violated
   * @param violationCode
   *          violation code
   * @param violationDescription
   *          description of violation
   * @param numberOfViolatingMutations
   *          number of mutations that produced this particular violation
   */
  public ConstraintViolationSummary(String constrainClass, short violationCode, String violationDescription, long numberOfViolatingMutations) {
    this.constrainClass = constrainClass;
    this.violationCode = violationCode;
    this.violationDescription = violationDescription;
    this.numberOfViolatingMutations = numberOfViolatingMutations;
  }

  /**
   * Creates a new summary from Thrift.
   *
   * @param tcvs
   *          Thrift summary
   */
  public ConstraintViolationSummary(TConstraintViolationSummary tcvs) {
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
    return String.format("ConstraintViolationSummary(constrainClass:%s, violationCode:%d, violationDescription:%s, numberOfViolatingMutations:%d)",
        constrainClass, violationCode, violationDescription, numberOfViolatingMutations);
  }

  /**
   * Converts this summary to Thrift.
   *
   * @return Thrift summary
   */
  public TConstraintViolationSummary toThrift() {
    return new TConstraintViolationSummary(this.constrainClass, violationCode, violationDescription, numberOfViolatingMutations);
  }

}
