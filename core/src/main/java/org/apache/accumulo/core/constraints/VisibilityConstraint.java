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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.VisibilityEvaluator;
import org.apache.accumulo.core.security.VisibilityParseException;
import org.apache.accumulo.core.util.BadArgumentException;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A constraint that checks the visibility of columns against the actor's authorizations. Violation
 * codes:
 * <ul>
 * <li>1 = failure to parse visibility expression</li>
 * <li>2 = insufficient authorization</li>
 * </ul>
 *
 * @deprecated since 2.1.0 Use
 *             {@link org.apache.accumulo.core.data.constraints.VisibilityConstraint}
 */
@Deprecated(since = "2.1.0")
@SuppressFBWarnings(value = "NM_SAME_SIMPLE_NAME_AS_SUPERCLASS",
    justification = "Same name used for compatibility during deprecation cycle")
public class VisibilityConstraint
    extends org.apache.accumulo.core.data.constraints.VisibilityConstraint implements Constraint {

  @Override
  public String getViolationDescription(short violationCode) {
    switch (violationCode) {
      case 1:
        return "Malformed column visibility";
      case 2:
        return "User does not have authorization on column visibility";
    }

    return null;
  }

  @Override
  public List<Short> check(Constraint.Environment env, Mutation mutation) {
    List<ColumnUpdate> updates = mutation.getUpdates();

    HashSet<String> ok = null;
    if (updates.size() > 1) {
      ok = new HashSet<>();
    }

    VisibilityEvaluator ve = null;

    for (ColumnUpdate update : updates) {

      byte[] cv = update.getColumnVisibility();
      if (cv.length > 0) {
        String key = null;
        if (ok != null && ok.contains(key = new String(cv, UTF_8))) {
          continue;
        }

        try {

          if (ve == null) {
            ve = new VisibilityEvaluator(env.getAuthorizationsContainer());
          }

          if (!ve.evaluate(new ColumnVisibility(cv))) {
            return Collections.singletonList((short) 2);
          }

        } catch (BadArgumentException | VisibilityParseException bae) {
          return Collections.singletonList((short) 1);
        }

        if (ok != null) {
          ok.add(key);
        }
      }
    }

    return null;
  }
}
