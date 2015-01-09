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
package org.apache.accumulo.server.replication;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import org.apache.accumulo.core.replication.ReplicationConstants;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.junit.Test;

/**
 *
 */
public class ZooKeeperInitializationTest {

  @Test
  public void parentNodesAreCreatedWhenMissing() throws Exception {
    ZooReaderWriter zReaderWriter = createMock(ZooReaderWriter.class);
    String zRoot = "/accumulo";

    expect(zReaderWriter.exists(zRoot + ReplicationConstants.ZOO_TSERVERS, null)).andReturn(false).once();
    zReaderWriter.mkdirs(zRoot + ReplicationConstants.ZOO_TSERVERS);
    expectLastCall().once();

    expect(zReaderWriter.exists(zRoot + ReplicationConstants.ZOO_WORK_QUEUE, null)).andReturn(false).once();
    zReaderWriter.mkdirs(zRoot + ReplicationConstants.ZOO_WORK_QUEUE);
    expectLastCall().once();

    replay(zReaderWriter);

    ZooKeeperInitialization.ensureZooKeeperInitialized(zReaderWriter, zRoot);

    verify(zReaderWriter);
  }

  @Test
  public void parentNodesAreNotRecreatedWhenAlreadyExist() throws Exception {
    ZooReaderWriter zReaderWriter = createMock(ZooReaderWriter.class);
    String zRoot = "/accumulo";

    expect(zReaderWriter.exists(zRoot + ReplicationConstants.ZOO_TSERVERS, null)).andReturn(true).once();

    expect(zReaderWriter.exists(zRoot + ReplicationConstants.ZOO_WORK_QUEUE, null)).andReturn(true).once();

    replay(zReaderWriter);

    ZooKeeperInitialization.ensureZooKeeperInitialized(zReaderWriter, zRoot);

    verify(zReaderWriter);
  }

}
