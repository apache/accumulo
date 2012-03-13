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
package org.apache.accumulo.examples.wikisearch.function;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.Logger;

public class QueryFunctions {
  
  protected static Logger log = Logger.getLogger(QueryFunctions.class);
  
  public static boolean between(String fieldValue, double left, double right) {
    try {
      Double value = Double.parseDouble(fieldValue);
      if (value >= left && value <= right)
        return true;
      return false;
    } catch (NumberFormatException nfe) {
      return false;
    }
  }
  
  public static boolean between(String fieldValue, long left, long right) {
    try {
      Long value = Long.parseLong(fieldValue);
      if (value >= left && value <= right)
        return true;
      return false;
    } catch (NumberFormatException nfe) {
      return false;
    }
  }
  
  public static Number abs(String fieldValue) {
    Number retval = null;
    try {
      Number value = NumberUtils.createNumber(fieldValue);
      if (null == value)
        retval = (Number) Integer.MIN_VALUE;
      else if (value instanceof Long)
        retval = Math.abs(value.longValue());
      else if (value instanceof Double)
        retval = Math.abs(value.doubleValue());
      else if (value instanceof Float)
        retval = Math.abs(value.floatValue());
      else if (value instanceof Integer)
        retval = Math.abs(value.intValue());
    } catch (NumberFormatException nfe) {
      return (Number) Integer.MIN_VALUE;
    }
    return retval;
  }
  
}
