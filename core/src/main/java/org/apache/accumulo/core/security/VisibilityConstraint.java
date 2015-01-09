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
package org.apache.accumulo.core.security;

import static com.google.common.base.Charsets.UTF_8;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.accumulo.core.constraints.Constraint;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.util.BadArgumentException;

/**
 * A constraint that checks the visibility of columns against the actor's authorizations. Violation codes:
 * <p>
 * <ul>
 * <li>1 = failure to parse visibility expression</li>
 * <li>2 = insufficient authorization</li>
 * </ul>
 */
public class VisibilityConstraint implements Constraint {

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
  public List<Short> check(Environment env, Mutation mutation) {
    List<ColumnUpdate> updates = mutation.getUpdates();

    HashSet<String> ok = null;
    if (updates.size() > 1)
      ok = new HashSet<String>();

    VisibilityEvaluator ve = null;

    for (ColumnUpdate update : updates) {

      byte[] cv = update.getColumnVisibility();
      if (cv.length > 0) {
        String key = null;
        if (ok != null && ok.contains(key = new String(cv, UTF_8)))
          continue;

        try {

          if (ve == null)
            ve = new VisibilityEvaluator(env);

          if (!ve.evaluate(new ColumnVisibility(cv)))
            return Collections.singletonList(Short.valueOf((short) 2));

        } catch (BadArgumentException bae) {
          return Collections.singletonList(new Short((short) 1));
        } catch (VisibilityParseException e) {
          return Collections.singletonList(new Short((short) 1));
        }

        if (ok != null)
          ok.add(key);
      }
    }

    return null;
  }
}
