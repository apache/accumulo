/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test.constraints;

import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.constraints.Constraint;

/**
 * Ensure that mutations are a reasonable size: we must be able to fit several in memory at a time.
 */
public class MaxMutationSize implements Constraint {
  static final long MAX_SIZE = Runtime.getRuntime().maxMemory() >> 8;
  static final List<Short> empty = Collections.emptyList();
  static final List<Short> violations = Collections.singletonList((short) 0);

  @Override
  public String getViolationDescription(short violationCode) {
    return String.format("mutation exceeded maximum size of %d", MAX_SIZE);
  }

  @Override
  public List<Short> check(Environment env, Mutation mutation) {
    if (mutation.estimatedMemoryUsed() < MAX_SIZE) {
      return empty;
    }
    return violations;
  }
}
