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
package org.apache.accumulo.examples.simple.combiner;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * This combiner calculates the max, min, sum, and count of long integers represented as strings in values. It stores the result in a comma-separated value of
 * the form min,max,sum,count. If such a value is encountered while combining, its information is incorporated into the running calculations of min, max, sum,
 * and count. See {@link Combiner} for more information on which values are combined together. See docs/examples/README.combiner for instructions.
 */
public class StatsCombiner extends Combiner {

  public static final String RADIX_OPTION = "radix";

  private int radix = 10;

  @Override
  public Value reduce(Key key, Iterator<Value> iter) {

    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    long sum = 0;
    long count = 0;

    while (iter.hasNext()) {
      String stats[] = iter.next().toString().split(",");

      if (stats.length == 1) {
        long val = Long.parseLong(stats[0], radix);
        min = Math.min(val, min);
        max = Math.max(val, max);
        sum += val;
        count += 1;
      } else {
        min = Math.min(Long.parseLong(stats[0], radix), min);
        max = Math.max(Long.parseLong(stats[1], radix), max);
        sum += Long.parseLong(stats[2], radix);
        count += Long.parseLong(stats[3], radix);
      }
    }

    String ret = Long.toString(min, radix) + "," + Long.toString(max, radix) + "," + Long.toString(sum, radix) + "," + Long.toString(count, radix);
    return new Value(ret.getBytes());
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);

    if (options.containsKey(RADIX_OPTION))
      radix = Integer.parseInt(options.get(RADIX_OPTION));
    else
      radix = 10;
  }

  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = super.describeOptions();
    io.setName("statsCombiner");
    io.setDescription("Combiner that keeps track of min, max, sum, and count");
    io.addNamedOption(RADIX_OPTION, "radix/base of the numbers");
    return io;
  }

  @Override
  public boolean validateOptions(Map<String,String> options) {
    if (!super.validateOptions(options))
      return false;

    if (options.containsKey(RADIX_OPTION) && !options.get(RADIX_OPTION).matches("\\d+"))
      throw new IllegalArgumentException("invalid option " + RADIX_OPTION + ":" + options.get(RADIX_OPTION));

    return true;
  }

  /**
   * A convenience method for setting the expected base/radix of the numbers
   *
   * @param iterConfig
   *          Iterator settings to configure
   * @param base
   *          The expected base/radix of the numbers.
   */
  public static void setRadix(IteratorSetting iterConfig, int base) {
    iterConfig.addOption(RADIX_OPTION, base + "");
  }
}
