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
package org.apache.accumulo.test.continuous;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.cli.ClientOpts.TimeConverter;
import org.apache.accumulo.core.cli.Help;

import com.beust.jcommander.Parameter;

public class TimeBinner {

  enum Operation {
    AVG, SUM, MIN, MAX, COUNT, CUMULATIVE, AMM, // avg,min,max
    AMM_HACK1 // special case
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

  static class Opts extends Help {
    @Parameter(names = "--period", description = "period", converter = TimeConverter.class, required = true)
    long period = 0;
    @Parameter(names = "--timeColumn", description = "time column", required = true)
    int timeColumn = 0;
    @Parameter(names = "--dataColumn", description = "data column", required = true)
    int dataColumn = 0;
    @Parameter(names = "--operation", description = "one of: AVG, SUM, MIN, MAX, COUNT", required = true)
    String operation;
    @Parameter(names = "--dateFormat", description = "a SimpleDataFormat string that describes the data format")
    String dateFormat = "MM/dd/yy-HH:mm:ss";
  }

  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(TimeBinner.class.getName(), args);

    Operation operation = Operation.valueOf(opts.operation);
    SimpleDateFormat sdf = new SimpleDateFormat(opts.dateFormat);

    BufferedReader in = new BufferedReader(new InputStreamReader(System.in, UTF_8));

    String line = null;

    HashMap<Long,DoubleWrapper> aggregation1 = new HashMap<Long,DoubleWrapper>();
    HashMap<Long,DoubleWrapper> aggregation2 = new HashMap<Long,DoubleWrapper>();
    HashMap<Long,DoubleWrapper> aggregation3 = new HashMap<Long,DoubleWrapper>();
    HashMap<Long,DoubleWrapper> aggregation4 = new HashMap<Long,DoubleWrapper>();

    while ((line = in.readLine()) != null) {

      try {
        String tokens[] = line.split("\\s+");

        long time = (long) Double.parseDouble(tokens[opts.timeColumn]);
        double data = Double.parseDouble(tokens[opts.dataColumn]);

        time = (time / opts.period) * opts.period;

        switch (operation) {
          case AMM_HACK1: {
            if (opts.dataColumn < 2) {
              throw new IllegalArgumentException("--dataColumn must be at least 2");
            }
            double data_min = Double.parseDouble(tokens[opts.dataColumn - 2]);
            double data_max = Double.parseDouble(tokens[opts.dataColumn - 1]);

            updateMin(time, aggregation3, data, data_min);
            updateMax(time, aggregation4, data, data_max);

            increment(time, aggregation1, data);
            increment(time, aggregation2, 1);
            break;
          }
          case AMM: {
            updateMin(time, aggregation3, data, data);
            updateMax(time, aggregation4, data, data);

            increment(time, aggregation1, data);
            increment(time, aggregation2, 1);
            break;
          }
          case AVG: {
            increment(time, aggregation1, data);
            increment(time, aggregation2, 1);
            break;
          }
          case MAX: {
            updateMax(time, aggregation1, data, data);
            break;
          }
          case MIN: {
            updateMin(time, aggregation1, data, data);
            break;
          }
          case COUNT: {
            increment(time, aggregation1, 1);
            break;
          }
          case SUM:
          case CUMULATIVE: {
            increment(time, aggregation1, data);
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
      String value;

      switch (operation) {
        case AMM_HACK1:
        case AMM: {
          DoubleWrapper countdw = aggregation2.get(entry.getKey());
          value = "" + (entry.getValue().d / countdw.d) + " " + aggregation3.get(entry.getKey()).d + " " + aggregation4.get(entry.getKey()).d;
          break;
        }
        case AVG: {
          DoubleWrapper countdw = aggregation2.get(entry.getKey());
          value = "" + (entry.getValue().d / countdw.d);
          break;
        }
        case CUMULATIVE: {
          cumulative += entry.getValue().d;
          value = "" + cumulative;
          break;
        }
        default:
          value = "" + entry.getValue().d;
      }

      System.out.println(sdf.format(new Date(entry.getKey())) + " " + value);
    }

  }

  private static void increment(long time, HashMap<Long,DoubleWrapper> aggregation, double amount) {
    get(time, aggregation, 0).d += amount;
  }

  private static void updateMax(long time, HashMap<Long,DoubleWrapper> aggregation, double data, double new_max) {
    DoubleWrapper maxdw = get(time, aggregation, Double.NEGATIVE_INFINITY);
    if (data > maxdw.d)
      maxdw.d = new_max;
  }

  private static void updateMin(long time, HashMap<Long,DoubleWrapper> aggregation, double data, double new_min) {
    DoubleWrapper mindw = get(time, aggregation, Double.POSITIVE_INFINITY);
    if (data < mindw.d)
      mindw.d = new_min;
  }
}
