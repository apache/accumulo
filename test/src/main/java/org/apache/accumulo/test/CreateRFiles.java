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
package org.apache.accumulo.test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.cli.Help;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

public class CreateRFiles {

  private static final Logger log = LoggerFactory.getLogger(CreateRFiles.class);

  static class Opts extends Help {

    @Parameter(names = "--output", description = "the destiation directory")
    String outputDirectory;

    @Parameter(names = "--numThreads", description = "number of threads to use when generating files")
    int numThreads = 4;

    @Parameter(names = "--start", description = "the start number for test data")
    long start = 0;

    @Parameter(names = "--end", description = "the maximum number for test data")
    long end = 10 * 1000 * 1000;

    @Parameter(names = "--splits", description = "the number of splits in the data")
    long numsplits = 4;
  }

  public static void main(String[] args) {
    Opts opts = new Opts();
    opts.parseArgs(CreateRFiles.class.getName(), args);

    long splitSize = Math.round((opts.end - opts.start) / (double) opts.numsplits);

    long currStart = opts.start;
    long currEnd = opts.start + splitSize;

    ExecutorService threadPool = Executors.newFixedThreadPool(opts.numThreads);

    int count = 0;
    while (currEnd <= opts.end && currStart < currEnd) {

      final String tia = String.format("--rfile %s/mf%05d --timestamp 1 --size 50 --random 56 --rows %d --start %d --user root", opts.outputDirectory, count,
          currEnd - currStart, currStart);

      Runnable r = new Runnable() {

        @Override
        public void run() {
          try {
            TestIngest.main(tia.split(" "));
          } catch (Exception e) {
            log.error("Could not run " + TestIngest.class.getName() + ".main using the input '" + tia + "'", e);
          }
        }

      };

      threadPool.execute(r);

      count++;
      currStart = currEnd;
      currEnd = Math.min(opts.end, currStart + splitSize);
    }

    threadPool.shutdown();
    while (!threadPool.isTerminated())
      try {
        threadPool.awaitTermination(1, TimeUnit.HOURS);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
  }
}
