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
package org.apache.accumulo.instamo;

import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.test.MiniAccumuloCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * An example unit test that shows how to use MiniAccumuloCluster in a unit test
 */

public class ExampleAccumuloUnitTest {
  
  public static TemporaryFolder folder = new TemporaryFolder();
  
  private static MiniAccumuloCluster accumulo;

  @BeforeClass
  public static void setupMiniCluster() throws Exception {

    folder.create();
    
    accumulo = new MiniAccumuloCluster(folder.getRoot(), "superSecret");
    
    accumulo.start();
    
  }

  @Test(timeout = 30000)
  public void test() throws Exception {
    AccumuloApp.run(accumulo.getInstanceName(), accumulo.getZooKeepers(), new PasswordToken("superSecret"), new String[0]);
  }
  
  @AfterClass
  public static void tearDownMiniCluster() throws Exception {
    accumulo.stop();
    folder.delete();
  }
  
}
