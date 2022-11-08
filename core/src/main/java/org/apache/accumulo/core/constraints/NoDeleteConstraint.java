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
package org.apache.accumulo.core.constraints;

import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * This constraint ensures mutations do not have deletes.
 *
 * @since 2.0.0
 * @deprecated since 2.1.0 Use {@link org.apache.accumulo.core.data.constraints.NoDeleteConstraint}
 */
@Deprecated(since = "2.1.0")
@SuppressFBWarnings(value = "NM_SAME_SIMPLE_NAME_AS_SUPERCLASS",
    justification = "Same name used for compatibility during deprecation cycle")
public class NoDeleteConstraint extends org.apache.accumulo.core.data.constraints.NoDeleteConstraint
    implements Constraint {

  @Override
  public String getViolationDescription(short violationCode) {
    if (violationCode == 1) {
      return "Deletes are not allowed";
    }
    return null;
  }

  @Override
  public List<Short> check(Constraint.Environment env, Mutation mutation) {
    List<ColumnUpdate> updates = mutation.getUpdates();
    for (ColumnUpdate update : updates) {
      if (update.isDeleted()) {
        return Collections.singletonList((short) 1);
      }
    }
    return null;
  }

}
