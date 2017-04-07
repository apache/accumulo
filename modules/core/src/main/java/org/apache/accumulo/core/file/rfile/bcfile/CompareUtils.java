/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.accumulo.core.file.rfile.bcfile;

import java.io.Serializable;
import java.util.Comparator;

class CompareUtils {
  /**
   * Prevent the instantiation of class.
   */
  private CompareUtils() {
    // nothing
  }

  /**
   * Interface for all objects that has a single integer magnitude.
   */
  interface Scalar {
    long magnitude();
  }

  static final class ScalarLong implements Scalar {
    private long magnitude;

    public ScalarLong(long m) {
      magnitude = m;
    }

    @Override
    public long magnitude() {
      return magnitude;
    }
  }

  public static final class ScalarComparator implements Comparator<Scalar>, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public int compare(Scalar o1, Scalar o2) {
      long diff = o1.magnitude() - o2.magnitude();
      if (diff < 0)
        return -1;
      if (diff > 0)
        return 1;
      return 0;
    }
  }

}
