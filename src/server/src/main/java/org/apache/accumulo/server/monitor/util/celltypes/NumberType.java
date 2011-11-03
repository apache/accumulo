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
package org.apache.accumulo.server.monitor.util.celltypes;

public class NumberType<T extends Number> extends CellType<T> {
  
  private T warnMin, warnMax, errMin, errMax;
  
  public NumberType(T warnMin, T warnMax, T errMin, T errMax) {
    this.warnMin = warnMin;
    this.warnMax = warnMax;
    this.errMin = errMin;
    this.errMax = errMax;
  }
  
  public NumberType(T errMin, T errMax) {
    this(null, null, errMin, errMax);
  }
  
  public NumberType() {
    this(null, null);
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public String format(Object obj) {
    T number = (T) obj;
    String s = "-";
    if (number instanceof Double || number instanceof Float) {
      if (warnMin != null && warnMax != null && errMin != null && errMax != null)
        s = commas(number.doubleValue(), warnMin.doubleValue(), warnMax.doubleValue(), errMin.doubleValue(), errMax.doubleValue());
      else if (errMin != null && errMax != null)
        s = commas(number.doubleValue(), errMin.doubleValue(), errMax.doubleValue());
      else
        s = commas(number.doubleValue());
    } else if (number instanceof Long || number instanceof Integer || number instanceof Short || number instanceof Byte) {
      if (warnMin != null && warnMax != null && errMin != null && errMax != null)
        s = commas(number.longValue(), warnMin.longValue(), warnMax.longValue(), errMin.longValue(), errMax.longValue());
      else if (errMin != null && errMax != null)
        s = commas(number.longValue(), errMin.longValue(), errMax.longValue());
      else
        s = commas(number.longValue());
    } else {
      if (number != null)
        s = String.valueOf(number);
    }
    return s;
  }
  
  @Override
  public int compare(T o1, T o2) {
    if (o1 == null && o2 == null)
      return 0;
    else if (o1 == null)
      return -1;
    else if (o2 == null)
      return 1;
    else
      return Double.valueOf(o1.doubleValue()).compareTo(o2.doubleValue());
  }
  
  public static String commas(long i) {
    return bigNumber(i);
  }
  
  public static String commas(long i, long errMin, long errMax) {
    if (i < errMin || i > errMax)
      return String.format("<span class='error'>%s</span>", bigNumber(i));
    return bigNumber(i);
  }
  
  public static String commas(double i) {
    return bigNumber((long) i);
  }
  
  public static String commas(double d, double errMin, double errMax) {
    if (d < errMin || d > errMax)
      return String.format("<span class='error'>%s</span>", bigNumber(d));
    return bigNumber(d);
  }
  
  public static String commas(long i, long warnMin, long warnMax, long errMin, long errMax) {
    if (i < errMin || i > errMax)
      return String.format("<span class='error'>%s</span>", bigNumber(i));
    if (i < warnMin || i > warnMax)
      return String.format("<span class='warning'>%s</span>", bigNumber(i));
    return bigNumber(i);
  }
  
  public static String commas(double d, double warnMin, double warnMax, double errMin, double errMax) {
    if (d < errMin || d > errMax)
      return String.format("<span class='error'>%s</span>", bigNumber(d));
    if (d < warnMin || d > warnMax)
      return String.format("<span class='warning'>%s</span>", bigNumber(d));
    return bigNumber(d);
  }
  
  private static final String COUNTS[] = {"", "K", "M", "B", "T", "e15", "e18", "e21"};
  
  public static String bigNumber(long big) {
    return bigNumber(big, COUNTS, 1000);
  }
  
  public static String bigNumber(double big) {
    return bigNumber(big, COUNTS, 1000);
  }
  
  public static String bigNumber(long big, String[] SUFFIXES, long base) {
    float divisor = 1;
    for (int i = 0; i < SUFFIXES.length; i++) {
      if (big / divisor < 1024) {
        if (i == 0)
          return String.format("%,d", big);
        return String.format("%,.2f%s", big / divisor, SUFFIXES[i]);
      }
      divisor *= base;
    }
    return String.format("%,.2f%s", big / divisor, SUFFIXES[SUFFIXES.length - 1]);
  }
  
  public static String bigNumber(double big, String[] SUFFIXES, long base) {
    float divisor = 1;
    for (int i = 0; i < SUFFIXES.length; i++) {
      if (big / divisor < 1024)
        return String.format("%,.2f%s", big / divisor, SUFFIXES[i]);
      divisor *= base;
    }
    return String.format("%,.2f%s", big / divisor, SUFFIXES[SUFFIXES.length - 1]);
  }
  
  @Override
  public String alignment() {
    return "right";
  }
  
}
