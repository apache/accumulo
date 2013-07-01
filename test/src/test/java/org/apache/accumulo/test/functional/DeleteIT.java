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

import static org.junit.Assert.assertEquals;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.server.util.Admin;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.TestRandomDeletes;
import org.apache.accumulo.test.VerifyIngest;
import org.junit.Test;

public class DeleteIT extends MacTest {
  
  @Test(timeout=30*1000)
  public void test() throws Exception {
    Connector c = getConnector();
    VerifyIngest.Opts vopts = new VerifyIngest.Opts();
    TestIngest.Opts opts = new TestIngest.Opts();
    vopts.rows = opts.rows = 10000;
    vopts.cols = opts.cols = 1;
    vopts.random = opts.random = 56;
    opts.createTable = true;
    TestIngest.ingest(c, opts, new BatchWriterOpts());
    assertEquals(0, cluster.exec(TestRandomDeletes.class, "-p", MacTest.PASSWORD, "-i", cluster.getInstanceName(), "-z", cluster.getZooKeepers()).waitFor());
    TestIngest.ingest(c, opts, new BatchWriterOpts());
    VerifyIngest.verifyIngest(c, vopts, new ScannerOpts());
    assertEquals(0, cluster.exec(Admin.class, "stopAll").waitFor());
  }
  
}
