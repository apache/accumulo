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
package org.apache.accumulo.tserver;

import java.util.List;

import org.apache.accumulo.core.constraints.Violations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.tserver.tablet.CommitSession;

public class TConstraintViolationException extends Exception {
  private static final long serialVersionUID = 1L;
  private final Violations violations;
  private final List<Mutation> violators;
  private final List<Mutation> nonViolators;
  private final CommitSession commitSession;

  public TConstraintViolationException(Violations violations, List<Mutation> violators, List<Mutation> nonViolators, CommitSession commitSession) {
    this.violations = violations;
    this.violators = violators;
    this.nonViolators = nonViolators;
    this.commitSession = commitSession;
  }

  Violations getViolations() {
    return violations;
  }

  List<Mutation> getViolators() {
    return violators;
  }

  List<Mutation> getNonViolators() {
    return nonViolators;
  }

  CommitSession getCommitSession() {
    return commitSession;
  }
}
