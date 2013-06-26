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

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.junit.Test;

public class RenameIT extends MacTest {
  
  @Test(timeout=60*1000)
  public void renameTest() throws Exception {
    BatchWriterOpts bwOpts = new BatchWriterOpts();
    ScannerOpts scanOpts = new ScannerOpts();
    TestIngest.Opts opts = new TestIngest.Opts();
    opts.createTable = true;
    Connector c = getConnector();
    TestIngest.ingest(c, opts, bwOpts);
    c.tableOperations().rename("test_ingest", "renamed");
    TestIngest.ingest(c, opts, bwOpts);
    VerifyIngest.Opts vopts = new VerifyIngest.Opts();
    VerifyIngest.verifyIngest(c, vopts, scanOpts);
    c.tableOperations().delete("test_ingest");
    c.tableOperations().rename("renamed", "test_ingest");
    VerifyIngest.verifyIngest(c, vopts, scanOpts);
  }
  
}
