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
package org.apache.accumulo.test.performance.scan;

import static org.junit.Assert.assertEquals;

import org.apache.accumulo.core.cli.ScannerOpts;
import org.junit.Test;

/**
 * Created by etcoleman on 10/11/16.
 */
public class CollectTabletStatsTest {

  @Test
  public void paramsDefaulThreadTest() {

    String tablename = "aTable";

    String[] args = {"-t", tablename, "--iterations", "2"};

    final CollectTabletStats.CollectOptions opts = new CollectTabletStats.CollectOptions();
    final ScannerOpts scanOpts = new ScannerOpts();
    opts.parseArgs(CollectTabletStats.class.getName(), args, scanOpts);

    assertEquals("Check iterations is set, default is 3", 2, opts.iterations);
    assertEquals("Check tablename is set", 0, tablename.compareTo(opts.getTableName()));
    assertEquals("Check default numThreads", 1, opts.numThreads);

  }

  @Test
  public void paramsSetThreadsTest() {

    String tablename = "aTable";

    String[] args = {"-t", tablename, "--iterations", "2", "--numThreads", "99"};

    final CollectTabletStats.CollectOptions opts = new CollectTabletStats.CollectOptions();
    final ScannerOpts scanOpts = new ScannerOpts();
    opts.parseArgs(CollectTabletStats.class.getName(), args, scanOpts);

    assertEquals("Check iterations is set, default is 3", 2, opts.iterations);
    assertEquals("Check tablename is set", 0, tablename.compareTo(opts.getTableName()));
    assertEquals("Check numThreads is set", 99, opts.numThreads);

    System.out.println(opts.columns);
  }

}
