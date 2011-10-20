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
package org.apache.accumulo.server.test.continuous;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;

public class TimeBinner {
  
  enum Operation {
    AVG, SUM, MIN, MAX, COUNT, CUMULATIVE,
  }
  
  private static class DoubleWrapper {
    double d;
  }
  
  private static DoubleWrapper get(long l, HashMap<Long,DoubleWrapper> m, double init) {
    DoubleWrapper dw = m.get(l);
    if (dw == null) {
      dw = new DoubleWrapper();
      dw.d = init;
      m.put(l, dw);
    }
    return dw;
  }
  
  public static void main(String[] args) throws Exception {
    
    if (args.length != 5) {
      System.out.println("usage : " + TimeBinner.class.getName() + " <period (seconds)> <time column> <data column> AVG|SUM|MIN|MAX|COUNT <date format>");
      System.exit(-1);
    }
    
    long period = Long.parseLong(args[0]) * 1000;
    int timeColumn = Integer.parseInt(args[1]);
    int dataColumn = Integer.parseInt(args[2]);
    Operation operation = Operation.valueOf(args[3]);
    SimpleDateFormat sdf = new SimpleDateFormat(args[4]);
    
    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
    
    String line = null;
    
    HashMap<Long,DoubleWrapper> aggregation1 = new HashMap<Long,DoubleWrapper>();
    HashMap<Long,DoubleWrapper> aggregation2 = new HashMap<Long,DoubleWrapper>();
    
    while ((line = in.readLine()) != null) {
      
      try {
        String tokens[] = line.split("\\s+");
        
        long time = (long) Double.parseDouble(tokens[timeColumn]);
        double data = Double.parseDouble(tokens[dataColumn]);
        
        time = (time / period) * period;
        
        switch (operation) {
          case AVG: {
            DoubleWrapper sumdw = get(time, aggregation1, 0);
            DoubleWrapper countdw = get(time, aggregation2, 0);
            
            sumdw.d += data;
            countdw.d++;
            
            break;
          }
          case MAX: {
            DoubleWrapper maxdw = get(time, aggregation1, Double.NEGATIVE_INFINITY);
            if (data > maxdw.d) {
              maxdw.d = data;
            }
            break;
          }
          case MIN: {
            DoubleWrapper mindw = get(time, aggregation1, Double.POSITIVE_INFINITY);
            if (data < mindw.d) {
              mindw.d = data;
            }
            break;
          }
          case COUNT: {
            DoubleWrapper countdw = get(time, aggregation1, 0);
            countdw.d++;
            break;
          }
          case SUM:
          case CUMULATIVE: {
            DoubleWrapper sumdw = get(time, aggregation1, 0);
            sumdw.d += data;
            break;
          }
        }
        
      } catch (Exception e) {
        System.err.println("Failed to process line : " + line + "  " + e.getMessage());
      }
    }
    
    TreeMap<Long,DoubleWrapper> sorted = new TreeMap<Long,DoubleWrapper>(aggregation1);
    
    Set<Entry<Long,DoubleWrapper>> es = sorted.entrySet();
    
    double cumulative = 0;
    for (Entry<Long,DoubleWrapper> entry : es) {
      double value;
      
      switch (operation) {
        case AVG: {
          DoubleWrapper countdw = aggregation2.get(entry.getKey());
          value = entry.getValue().d / countdw.d;
          break;
        }
        case CUMULATIVE: {
          cumulative += entry.getValue().d;
          value = cumulative;
          break;
        }
        default:
          value = entry.getValue().d;
      }
      
      System.out.println(sdf.format(new Date(entry.getKey())) + " " + value);
    }
    
  }
}
