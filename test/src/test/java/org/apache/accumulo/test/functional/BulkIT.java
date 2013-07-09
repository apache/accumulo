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
package org.apache.accumulo.test.functional;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.TestIngest.Opts;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class BulkIT extends MacTest {
  
  static final int N = 100000;
  static final int COUNT = 5;
  
  @Test(timeout=120*1000)
  public void test() throws Exception {
    Connector c = getConnector();
    c.tableOperations().create("test_ingest");
    FileSystem fs = FileSystem.get(CachedConfiguration.getInstance());
    String base = cluster.getConfig().getDir().getAbsolutePath();
    fs.mkdirs(new Path(base + "/testBulkFail"));
    
    Opts opts = new Opts();
    opts.timestamp = 1;
    opts.random = 56;
    opts.rows = N;
    opts.instance = cluster.getInstanceName();
    opts.cols = 1;
    for (int i = 0; i < COUNT; i++) {
      opts.outputFile = base + String.format("/testrf/rf%02d", i);
      opts.startRow = N * i;
      TestIngest.ingest(c, opts , BWOPTS);
    }
    opts.outputFile = base + String.format("/testrf/rf%02d", N);
    opts.startRow = N;
    opts.rows = 1;
    // create an rfile with one entry, there was a bug with this:
    TestIngest.ingest(c, opts , BWOPTS);
    c.tableOperations().importDirectory("test_ingest", base + "/testrf", base + "/testBulkFail", false);
    VerifyIngest.Opts vopts = new VerifyIngest.Opts();
    vopts.random = 56;
    for (int i = 0; i < COUNT; i++) {
      vopts.startRow = i * N;
      vopts.rows = N;
      VerifyIngest.verifyIngest(c, vopts, SOPTS);
    }
    vopts.startRow = N;
    vopts.rows = 1;
    VerifyIngest.verifyIngest(c, vopts, SOPTS);
  }
  
  
}
