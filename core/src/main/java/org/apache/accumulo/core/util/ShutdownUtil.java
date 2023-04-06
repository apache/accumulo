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

import java.io.IOException;

public class ShutdownUtil {

  /**
   * Determine if a JVM shutdown is in progress.
   *
   */
  public static boolean isShutdownInProgress() {
    try {
      Runtime.getRuntime().removeShutdownHook(new Thread(() -> {}));
    } catch (IllegalStateException ise) {
      return true;
    }

    return false;
  }

  public static boolean isIOException(Throwable e) {
    if (e == null) {
      return false;
    }

    if (e instanceof IOException) {
      return true;
    }

    for (Throwable suppressed : e.getSuppressed()) {
      if (isIOException(suppressed)) {
        return true;
      }
    }

    return isIOException(e.getCause());
  }

  /**
   * @return true if there is a possibility that the exception was caused by the hadoop shutdown
   *         hook closing the hadoop file system objects, otherwise false
   */
  public static boolean wasCausedByHadoopShutdown(Exception e) {
    return isShutdownInProgress() && isIOException(e);
  }

}
