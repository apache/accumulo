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
package org.apache.accumulo.tserver.constraints;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.constraints.Constraint;
import org.apache.accumulo.core.constraints.Constraint.Environment;
import org.apache.accumulo.core.constraints.Violations;
import org.apache.accumulo.core.data.ConstraintViolationSummary;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.impl.ComparableBytes;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

public class ConstraintChecker {

  private ArrayList<Constraint> constrains;
  private static final Logger log = LoggerFactory.getLogger(ConstraintChecker.class);

  private ClassLoader loader;
  private TableConfiguration conf;

  private AtomicLong lastCheck = new AtomicLong(0);

  public ConstraintChecker(TableConfiguration conf) {
    constrains = new ArrayList<>();

    this.conf = conf;

    try {
      String context = conf.get(Property.TABLE_CLASSPATH);

      if (context != null && !context.equals("")) {
        loader = AccumuloVFSClassLoader.getContextManager().getClassLoader(context);
      } else {
        loader = AccumuloVFSClassLoader.getClassLoader();
      }

      for (Entry<String,String> entry : conf) {
        if (entry.getKey().startsWith(Property.TABLE_CONSTRAINT_PREFIX.getKey())) {
          String className = entry.getValue();
          Class<? extends Constraint> clazz = loader.loadClass(className).asSubclass(Constraint.class);
          log.debug("Loaded constraint {} for {}", clazz.getName(), conf.getTableId());
          constrains.add(clazz.newInstance());
        }
      }

      lastCheck.set(System.currentTimeMillis());

    } catch (Throwable e) {
      constrains.clear();
      loader = null;
      constrains.add(new UnsatisfiableConstraint((short) -1, "Failed to load constraints, not accepting mutations."));
      log.error("Failed to load constraints " + conf.getTableId() + " " + e.toString(), e);
    }
  }

  @VisibleForTesting
  ArrayList<Constraint> getConstraints() {
    return constrains;
  }

  public boolean classLoaderChanged() {

    if (constrains.size() == 0)
      return false;

    try {
      String context = conf.get(Property.TABLE_CLASSPATH);

      ClassLoader currentLoader;

      if (context != null && !context.equals("")) {
        currentLoader = AccumuloVFSClassLoader.getContextManager().getClassLoader(context);
      } else {
        currentLoader = AccumuloVFSClassLoader.getClassLoader();
      }

      return currentLoader != loader;
    } catch (Exception e) {
      log.debug("Failed to check {}", e.getMessage());
      return true;
    }
  }

  private static Violations addViolation(Violations violations, ConstraintViolationSummary cvs) {
    if (violations == null) {
      violations = new Violations();
    }
    violations.add(cvs);
    return violations;
  }

  public Violations check(Environment env, Mutation m) {
    if (!env.getExtent().contains(new ComparableBytes(m.getRow()))) {
      Violations violations = new Violations();

      ConstraintViolationSummary cvs = new ConstraintViolationSummary(SystemConstraint.class.getName(), (short) -1, "Mutation outside of tablet extent", 1);
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
            violations = addViolation(violations, new ConstraintViolationSummary(className, vcode, constraint.getViolationDescription(vcode), 1));
          }
        }
      } catch (Throwable throwable) {
        log.warn("CONSTRAINT FAILED : {}", throwable.getMessage(), throwable);

        // constraint failed in some way, do not allow mutation to pass
        short vcode;
        String msg;

        if (throwable instanceof NullPointerException) {
          vcode = -1;
          msg = "threw NullPointerException";
        } else if (throwable instanceof ArrayIndexOutOfBoundsException) {
          vcode = -2;
          msg = "threw ArrayIndexOutOfBoundsException";
        } else if (throwable instanceof NumberFormatException) {
          vcode = -3;
          msg = "threw NumberFormatException";
        } else if (throwable instanceof IOException) {
          vcode = -4;
          msg = "threw IOException (or subclass of)";
        } else {
          vcode = -100;
          msg = "threw some Exception";
        }

        violations = addViolation(violations, new ConstraintViolationSummary(constraint.getClass().getName(), vcode, "CONSTRAINT FAILED : " + msg, 1));
      }
    }

    return violations;
  }
}
