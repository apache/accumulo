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
package org.apache.accumulo.cluster.standalone;

import org.apache.accumulo.core.master.thrift.MasterGoalState;
import org.apache.accumulo.minicluster.ServerType;
import org.easymock.EasyMock;
import org.junit.Test;

public class StandaloneAccumuloClusterTest {

  @Test
  public void test() throws Exception {
    StandaloneAccumuloCluster cluster = EasyMock.createMockBuilder(StandaloneAccumuloCluster.class).addMockedMethod("getClusterControl").createMock();
    StandaloneClusterControl control = EasyMock.createMock(StandaloneClusterControl.class);

    // Return our mocked clustercontrol
    EasyMock.expect(cluster.getClusterControl()).andReturn(control);

    // `SetGoalState NORMAL` should be called specifically on this method, not via ClusterControl.exec(..)
    control.setGoalState(MasterGoalState.NORMAL.toString());
    EasyMock.expectLastCall().once();

    // Start the procs
    for (ServerType type : StandaloneAccumuloCluster.ALL_SERVER_TYPES) {
      control.startAllServers(type);
    }

    // Switch to replay
    EasyMock.replay(cluster, control);

    // Call start on the cluster
    cluster.start();

    // Verify the expectations
    EasyMock.verify(cluster, control);
  }

}
