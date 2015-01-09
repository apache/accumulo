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
package org.apache.accumulo.core.constraints;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.data.ConstraintViolationSummary;

public class Violations {

  private static class CVSKey {
    private String className;
    private short vcode;

    CVSKey(ConstraintViolationSummary cvs) {
      this.className = cvs.constrainClass;
      this.vcode = cvs.violationCode;
    }

    @Override
    public int hashCode() {
      return className.hashCode() + vcode;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof CVSKey)
        return equals((CVSKey) o);
      return false;
    }

    public boolean equals(CVSKey ocvsk) {
      return className.equals(ocvsk.className) && vcode == ocvsk.vcode;
    }
  }

  private HashMap<CVSKey,ConstraintViolationSummary> cvsmap;

  public Violations() {
    cvsmap = new HashMap<CVSKey,ConstraintViolationSummary>();
  }

  public boolean isEmpty() {
    return cvsmap.isEmpty();
  }

  private void add(CVSKey cvsk, ConstraintViolationSummary cvs) {
    ConstraintViolationSummary existingCvs = cvsmap.get(cvsk);

    if (existingCvs == null) {
      cvsmap.put(cvsk, cvs);
    } else {
      existingCvs.numberOfViolatingMutations += cvs.numberOfViolatingMutations;
    }
  }

  public void add(ConstraintViolationSummary cvs) {
    CVSKey cvsk = new CVSKey(cvs);
    add(cvsk, cvs);
  }

  public void add(Violations violations) {
    Set<Entry<CVSKey,ConstraintViolationSummary>> es = violations.cvsmap.entrySet();

    for (Entry<CVSKey,ConstraintViolationSummary> entry : es) {
      add(entry.getKey(), entry.getValue());
    }

  }

  public void add(List<ConstraintViolationSummary> cvsList) {
    for (ConstraintViolationSummary constraintViolationSummary : cvsList) {
      add(constraintViolationSummary);
    }

  }

  public List<ConstraintViolationSummary> asList() {
    return new ArrayList<ConstraintViolationSummary>(cvsmap.values());
  }

}
