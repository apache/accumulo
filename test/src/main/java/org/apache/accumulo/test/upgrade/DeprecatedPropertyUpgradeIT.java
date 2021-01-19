/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test.upgrade;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.master.upgrade.Upgrader9to10;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.junit.Test;

public class DeprecatedPropertyUpgradeIT extends ConfigurableMacBase {
  private static final Upgrader9to10 upgrader = new Upgrader9to10();

  @Test
  public void testZookeeperUpdate() throws Exception {
    String oldProp = "master.bulk.retries";
    String newProp = Property.MANAGER_BULK_RETRIES.getKey();
    String propValue =
        Integer.toString(Integer.parseInt(Property.MANAGER_BULK_RETRIES.getDefaultValue()) * 2);

    String zPath = getServerContext().getZooKeeperRoot() + Constants.ZCONFIG + "/" + oldProp;
    getServerContext().getZooReaderWriter().putPersistentData(zPath, propValue.getBytes(UTF_8),
        ZooUtil.NodeExistsPolicy.OVERWRITE);

    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      Map<String,String> systemConfig = client.instanceOperations().getSystemConfiguration();
      assertTrue(oldProp + " missing from system config before upgrade",
          systemConfig.containsKey(oldProp));
      assertEquals(propValue, systemConfig.get(oldProp));
      assertEquals(Property.MANAGER_BULK_RETRIES.getDefaultValue(), systemConfig.get(newProp));
      assertNotEquals(propValue, systemConfig.get(newProp));

      upgrader.upgradeZookeeper(getServerContext());

      systemConfig = client.instanceOperations().getSystemConfiguration();
      assertFalse(oldProp + " is still set after upgrade", systemConfig.containsKey(oldProp));
      assertEquals(propValue, systemConfig.get(newProp));
    }
  }
}
