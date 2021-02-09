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
package org.apache.accumulo.test.functional;

import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.manager.Manager;
import org.junit.Test;

public class BackupManagerIT extends ConfigurableMacBase {

  @Override
  protected int defaultTimeoutSeconds() {
    return 120;
  }

  @Test
  public void test() throws Exception {
    // wait for manager
    UtilWaitThread.sleep(1000);
    // create a backup
    Process backup = exec(Manager.class);
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      String secret = getCluster().getSiteConfiguration().get(Property.INSTANCE_SECRET);
      ZooReaderWriter writer = new ZooReaderWriter(cluster.getZooKeepers(), 30 * 1000, secret);
      String root = "/accumulo/" + client.instanceOperations().getInstanceID();
      List<String> children;
      // wait for 2 lock entries
      do {
        UtilWaitThread.sleep(100);
        children = writer.getChildren(root + Constants.ZMANAGER_LOCK);
      } while (children.size() != 2);
      Collections.sort(children);
      // wait for the backup manager to learn to be the backup
      UtilWaitThread.sleep(1000);
      // generate a false zookeeper event
      String lockPath = root + Constants.ZMANAGER_LOCK + children.get(0);
      byte[] data = writer.getData(lockPath);
      writer.getZooKeeper().setData(lockPath, data, -1);
      // let it propagate
      UtilWaitThread.sleep(500);
      // kill the manager by removing its lock
      writer.recursiveDelete(lockPath, NodeMissingPolicy.FAIL);
      // ensure the backup becomes the manager
      client.tableOperations().create(getUniqueNames(1)[0]);
    } finally {
      backup.destroy();
    }
  }

}
