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
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloCluster.LogWriter;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.rules.TemporaryFolder;

public class MacTest {
  public static final Logger log = Logger.getLogger(MacTest.class);
  public static final String PASSWORD = "secret";
  static final ScannerOpts SOPTS = new ScannerOpts();
  static final BatchWriterOpts BWOPTS = new BatchWriterOpts();
  public TemporaryFolder folder = new TemporaryFolder();
  public MiniAccumuloCluster cluster;
  
  public Connector getConnector() throws AccumuloException, AccumuloSecurityException {
    return cluster.getConnector("root", PASSWORD);
  }
  
  @Before
  public void setUp() throws Exception {
    folder.create();
    MiniAccumuloConfig cfg = new MiniAccumuloConfig(folder.newFolder("miniAccumulo"), PASSWORD);
    configure(cfg);
    cluster = new MiniAccumuloCluster(cfg);
    cluster.start();
  }
  
  public void configure(MiniAccumuloConfig cfg) {
  }
  
  @After
  public void tearDown() throws Exception {
    if (cluster != null)
      cluster.stop();
    for (LogWriter log : cluster.getLogWriters())
      log.flush();
    folder.delete();
  }
  
}
