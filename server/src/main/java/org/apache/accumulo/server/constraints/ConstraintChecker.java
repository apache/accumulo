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
package org.apache.accumulo.server.constraints;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.constraints.Constraint;
import org.apache.accumulo.core.constraints.Violations;
import org.apache.accumulo.core.constraints.Constraint.Environment;
import org.apache.accumulo.core.data.ComparableBytes;
import org.apache.accumulo.core.data.ConstraintViolationSummary;
import org.apache.accumulo.core.data.Mutation;
import org.apache.log4j.Logger;

public class ConstraintChecker {
  
  private ArrayList<Constraint> constrains;
  private static final Logger log = Logger.getLogger(ConstraintChecker.class);
  
  public ConstraintChecker() {
    constrains = new ArrayList<Constraint>();
  }
  
  public void addConstraint(Constraint c) {
    constrains.add(c);
  }
  
  public Violations check(Environment env, Mutation m) {
    
    Violations violations = null;
    
    if (!env.getExtent().contains(new ComparableBytes(m.getRow()))) {
      violations = new Violations();
      
      ConstraintViolationSummary cvs = new ConstraintViolationSummary(SystemConstraint.class.getName(), (short) -1, "Mutation outside of tablet extent", 1);
      violations.add(cvs);
      
      // do not bother with further checks since this mutation does not go with this tablet
      return violations;
    }
    
    for (Constraint constraint : constrains) {
      List<Short> violationCodes = null;
      Throwable throwable = null;
      
      try {
        violationCodes = constraint.check(env, m);
      } catch (Throwable t) {
        throwable = t;
        log.warn("CONSTRAINT FAILED : " + throwable.getMessage(), t);
      }
      
      if (violationCodes != null) {
        for (Short vcode : violationCodes) {
          ConstraintViolationSummary cvs = new ConstraintViolationSummary(constraint.getClass().getName(), vcode, constraint.getViolationDescription(vcode), 1);
          if (violations == null)
            violations = new Violations();
          violations.add(cvs);
        }
      } else if (throwable != null) {
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
        
        ConstraintViolationSummary cvs = new ConstraintViolationSummary(constraint.getClass().getName(), vcode, "CONSTRAINT FAILED : " + msg, 1);
        if (violations == null)
          violations = new Violations();
        violations.add(cvs);
      }
    }
    
    return violations;
  }
}
