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
package org.apache.accumulo.core.util;

import java.util.HashSet;
import java.util.Set;

public class ClassUtil {

  /**
   * Utility method to return the set of all interfaces implemented by the supplied class and it's
   * parents.
   *
   * @param clazz Class object to check
   * @return Set of interface classes implemented by input argument
   */
  public static Set<Class<?>> getInterfaces(Class<?> clazz) {
    var set = new HashSet<Class<?>>();
    if (clazz != null) {
      set.addAll(getInterfaces(clazz.getSuperclass()));
      for (Class<?> interfaze : clazz.getInterfaces()) {
        set.add(interfaze);
      }
    }
    return set;
  }

}
