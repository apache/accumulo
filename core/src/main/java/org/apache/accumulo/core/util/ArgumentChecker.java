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
package org.apache.accumulo.core.util;

/**
 * This class provides methods to check arguments of a variable number for null values, or anything else that might be required on a routine basis. These
 * methods should be used for early failures as close to the end user as possible, so things do not fail later on the server side, when they are harder to
 * debug.
 * 
 * Methods are created for a specific number of arguments, due to the poor performance of array allocation for varargs methods.
 */
public class ArgumentChecker {
  private static final String NULL_ARG_MSG = "argument was null";
  
  public static final void notNull(final Object arg1) {
    if (arg1 == null)
      throw new IllegalArgumentException(NULL_ARG_MSG + ":Is null- arg1? " + (arg1 == null));
  }
  
  public static final void notNull(final Object arg1, final Object arg2) {
    if (arg1 == null || arg2 == null)
      throw new IllegalArgumentException(NULL_ARG_MSG + ":Is null- arg1? " + (arg1 == null) + " arg2? " + (arg2 == null));
  }
  
  public static final void notNull(final Object arg1, final Object arg2, final Object arg3) {
    if (arg1 == null || arg2 == null || arg3 == null)
      throw new IllegalArgumentException(NULL_ARG_MSG + ":Is null- arg1? " + (arg1 == null) + " arg2? " + (arg2 == null) + " arg3? " + (arg3 == null));
  }
  
  public static final void notNull(final Object arg1, final Object arg2, final Object arg3, final Object arg4) {
    if (arg1 == null || arg2 == null || arg3 == null || arg4 == null)
      throw new IllegalArgumentException(NULL_ARG_MSG + ":Is null- arg1? " + (arg1 == null) + " arg2? " + (arg2 == null) + " arg3? " + (arg3 == null)
          + " arg4? " + (arg4 == null));
  }
  
  public static final void notNull(final Object[] args) {
    if (args == null)
      throw new IllegalArgumentException(NULL_ARG_MSG + ":arg array is null");
    
    for (int i = 0; i < args.length; i++)
      if (args[i] == null)
        throw new IllegalArgumentException(NULL_ARG_MSG + ":arg" + i + " is null");
  }
  
  public static final void strictlyPositive(final int i) {
    if (i <= 0)
      throw new IllegalArgumentException("integer should be > 0, was " + i);
  }
}
