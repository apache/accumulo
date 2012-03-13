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

public class Stat {
  
  long max = Long.MIN_VALUE;
  long min = Long.MAX_VALUE;
  long sum = 0;
  int count = 0;
  double partialStdDev = 0;
  
  public void addStat(long stat) {
    if (stat > max)
      max = stat;
    if (stat < min)
      min = stat;
    
    sum += stat;
    
    partialStdDev += stat * stat;
    
    count++;
  }
  
  public long getMin() {
    return min;
  }
  
  public long getMax() {
    return max;
  }
  
  public double getAverage() {
    return ((double) sum) / count;
  }
  
  public double getStdDev() {
    return Math.sqrt(partialStdDev / count - getAverage() * getAverage());
  }
  
  public String toString() {
    return String.format("%,d %,d %,.2f %,d", getMin(), getMax(), getAverage(), count);
  }
  
  public void clear() {
    sum = 0;
    count = 0;
    partialStdDev = 0;
  }
  
  public long getSum() {
    return sum;
  }
}
