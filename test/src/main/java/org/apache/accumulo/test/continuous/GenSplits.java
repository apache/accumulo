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

import java.util.List;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

/**
 *
 */
public class GenSplits {

  static class Opts {
    @Parameter(names = "--min", description = "minimum row")
    long minRow = 0;

    @Parameter(names = "--max", description = "maximum row")
    long maxRow = Long.MAX_VALUE;

    @Parameter(description = "<num tablets>")
    List<String> args = null;
  }

  public static void main(String[] args) {

    Opts opts = new Opts();
    JCommander jcommander = new JCommander(opts);
    jcommander.setProgramName(GenSplits.class.getSimpleName());

    try {
      jcommander.parse(args);
    } catch (ParameterException pe) {
      System.err.println(pe.getMessage());
      jcommander.usage();
      System.exit(-1);
    }

    if (opts.args == null || opts.args.size() != 1) {
      jcommander.usage();
      System.exit(-1);
    }

    int numTablets = Integer.parseInt(opts.args.get(0));

    if (numTablets < 1) {
      System.err.println("ERROR: numTablets < 1");
      System.exit(-1);
    }

    if (opts.minRow >= opts.maxRow) {
      System.err.println("ERROR: min >= max");
      System.exit(-1);
    }

    int numSplits = numTablets - 1;
    long distance = ((opts.maxRow - opts.minRow) / numTablets) + 1;
    long split = distance;
    for (int i = 0; i < numSplits; i++) {

      String s = String.format("%016x", split + opts.minRow);

      while (s.charAt(s.length() - 1) == '0') {
        s = s.substring(0, s.length() - 1);
      }

      System.out.println(s);
      split += distance;
    }
  }
}
