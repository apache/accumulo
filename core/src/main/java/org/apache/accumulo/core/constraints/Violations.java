/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.constraints;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.ConstraintViolationSummary;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * This class is replaced by {@link org.apache.accumulo.core.data.constraints.Violations}
 *
 * @deprecated since 2.1.0 Use {@link org.apache.accumulo.core.data.constraints.Violations}
 */
@Deprecated(since = "2.1.0")
@SuppressFBWarnings(value = {"NM_SAME_SIMPLE_NAME_AS_SUPERCLASS"},
    justification = "Same name used for compatibility during deprecation cycle")
public class Violations extends org.apache.accumulo.core.data.constraints.Violations {

  public static final org.apache.accumulo.core.constraints.Violations EMPTY =
      new org.apache.accumulo.core.constraints.Violations(Collections.emptyMap());

  /**
   * Creates a new empty object.
   */
  public Violations() {
    super();
  }

  private Violations(Map<CVSKey,ConstraintViolationSummary> cvsmap) {
    super(cvsmap);
  }

  /**
   * Checks if this object is empty, i.e., that no violations have been added.
   *
   * @return true if empty
   */
  public boolean isEmpty() {
    return super.isEmpty();
  }

  /**
   * Adds a violation. If a matching violation was already added, then its count is increased.
   *
   * @param cvs
   *          summary of violation
   */
  public void add(ConstraintViolationSummary cvs) {
    super.add(cvs);
  }

  /**
   * Adds all violations from the given object to this one.
   *
   * @param violations
   *          violations to add
   */
  @SuppressFBWarnings(value = "NM_WRONG_PACKAGE",
      justification = "Same name used for compatibility during deprecation cycle")
  public void add(org.apache.accumulo.core.constraints.Violations violations) {
    super.add(violations);
  }

  /**
   * Adds a list of violations.
   *
   * @param cvsList
   *          list of violation summaries
   */
  public void add(List<ConstraintViolationSummary> cvsList) {
    super.add(cvsList);
  }

  /**
   * Gets the violations as a list of summaries.
   *
   * @return list of violation summaries
   */
  public List<ConstraintViolationSummary> asList() {
    return super.asList();
  }

}
