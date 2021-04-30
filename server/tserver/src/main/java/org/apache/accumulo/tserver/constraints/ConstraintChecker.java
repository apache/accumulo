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
package org.apache.accumulo.tserver.constraints;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.constraints.Violations;
import org.apache.accumulo.core.data.ConstraintViolationSummary;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.data.constraints.Constraint;
import org.apache.accumulo.core.data.constraints.Constraint.Environment;
import org.apache.accumulo.core.dataImpl.ComparableBytes;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.hadoop.io.BinaryComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

public class ConstraintChecker {

  private ArrayList<Constraint> constraints;
  private static final Logger log = LoggerFactory.getLogger(ConstraintChecker.class);

  public ConstraintChecker(AccumuloConfiguration conf) {
    constraints = new ArrayList<>();

    try {
      String context = ClassLoaderUtil.tableContext(conf);

      for (Entry<String,String> entry : conf
          .getAllPropertiesWithPrefix(Property.TABLE_CONSTRAINT_PREFIX).entrySet()) {
        if (entry.getKey().startsWith(Property.TABLE_CONSTRAINT_PREFIX.getKey())) {
          String className = entry.getValue();
          Class<? extends Constraint> clazz =
              ClassLoaderUtil.loadClass(context, className, Constraint.class);

          log.debug("Loaded constraint {} for {}", clazz.getName(),
              ((TableConfiguration) conf).getTableId());
          constraints.add(clazz.getDeclaredConstructor().newInstance());
        }
      }

    } catch (Exception e) {
      constraints.clear();
      constraints.add(new UnsatisfiableConstraint((short) -1,
          "Failed to load constraints, not accepting mutations."));
      log.error("Failed to load constraints " + ((TableConfiguration) conf).getTableId() + " " + e,
          e);
    }
  }

  @VisibleForTesting
  ArrayList<Constraint> getConstraints() {
    return constraints;
  }

  private static Violations addViolation(Violations violations, ConstraintViolationSummary cvs) {
    if (violations == null) {
      violations = new Violations();
    }
    violations.add(cvs);
    return violations;
  }

  public Violations check(Environment env, Mutation m) {
    if (!tabletContains(env.getTablet(), new ComparableBytes(m.getRow()))) {
      Violations violations = new Violations();

      ConstraintViolationSummary cvs = new ConstraintViolationSummary(
          SystemConstraint.class.getName(), (short) -1, "Mutation outside of tablet extent", 1);
      violations.add(cvs);

      // do not bother with further checks since this mutation does not go with this tablet
      return violations;
    }

    // violations is intentionally initialized as null for performance
    Violations violations = null;
    for (Constraint constraint : getConstraints()) {
      try {
        List<Short> violationCodes = constraint.check(env, m);
        if (violationCodes != null) {
          String className = constraint.getClass().getName();
          for (Short vcode : violationCodes) {
            violations = addViolation(violations, new ConstraintViolationSummary(className, vcode,
                constraint.getViolationDescription(vcode), 1));
          }
        }
      } catch (Exception e) {
        log.warn("CONSTRAINT FAILED : {}", e.getMessage(), e);

        // constraint failed in some way, do not allow mutation to pass
        short vcode;
        String msg;

        if (e instanceof NullPointerException) {
          vcode = -1;
          msg = "threw NullPointerException";
        } else if (e instanceof ArrayIndexOutOfBoundsException) {
          vcode = -2;
          msg = "threw ArrayIndexOutOfBoundsException";
        } else if (e instanceof NumberFormatException) {
          vcode = -3;
          msg = "threw NumberFormatException";
        } else {
          vcode = -100;
          msg = "threw some Exception";
        }

        violations =
            addViolation(violations, new ConstraintViolationSummary(constraint.getClass().getName(),
                vcode, "CONSTRAINT FAILED : " + msg, 1));
      }
    }

    return violations;
  }

  /**
   * Return true if the tablet contains the row. This is similar to the contains in KeyExtent
   */
  public boolean tabletContains(TabletId tablet, BinaryComparable row) {
    if (row == null) {
      throw new IllegalArgumentException(
          "Passing null to contains is ambiguous, could be in first or last extent of table");
    }
    return (tablet.getPrevEndRow() == null || tablet.getPrevEndRow().compareTo(row) < 0)
        && (tablet.getEndRow() == null || tablet.getEndRow().compareTo(row) >= 0);
  }
}
