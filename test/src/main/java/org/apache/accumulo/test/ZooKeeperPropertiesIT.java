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

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.junit.Test;

public class ZooKeeperPropertiesIT extends AccumuloClusterHarness {

  @Test(expected = AccumuloException.class)
  public void testNoFiles() throws Exception {
    try (AccumuloClient client = getAccumuloClient()) {
      // Should throw an error as this property can't be changed in ZooKeeper
      client.instanceOperations().setProperty(Property.GENERAL_RPC_TIMEOUT.getKey(), "60s");
    }
  }

}
