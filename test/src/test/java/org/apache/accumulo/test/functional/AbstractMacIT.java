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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.log4j.Logger;
import org.junit.rules.TemporaryFolder;

public abstract class AbstractMacIT {
  public static final Logger log = Logger.getLogger(AbstractMacIT.class);
      
  public static final String ROOT_PASSWORD = "secret";
  public static final ScannerOpts SOPTS = new ScannerOpts();
  public static final BatchWriterOpts BWOPTS = new BatchWriterOpts();
  
  public abstract MiniAccumuloCluster getCluster();
  
  protected static void cleanUp(MiniAccumuloCluster cluster, TemporaryFolder folder) {
    if (cluster != null)
      try {
        cluster.stop();
      } catch (Exception e) {}
    folder.delete();
  }
  
  static AtomicInteger tableCount = new AtomicInteger();
  static public String makeTableName() {
    return "table" + tableCount.getAndIncrement();
  }
  
  public Connector getConnector() throws AccumuloException, AccumuloSecurityException {
    return getCluster().getConnector("root", ROOT_PASSWORD);
  }
  
  public String rootPath() {
    return getCluster().getConfig().getDir().getAbsolutePath();
  }
  
  public Process exec(Class<? extends Object> clazz, String... args) throws IOException {
    return getCluster().exec(clazz, args);
  }
}
