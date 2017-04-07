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
package org.apache.accumulo.fate.zookeeper;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.fate.zookeeper.IZooReaderWriter.Mutator;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.BadVersionException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ZooReaderWriterTest {

  private ZooReaderWriter zrw;
  private ZooKeeper zk;
  private RetryFactory retryFactory;
  private Retry retry;

  @Before
  public void setup() {
    zk = EasyMock.createMock(ZooKeeper.class);
    zrw = EasyMock.createMockBuilder(ZooReaderWriter.class).addMockedMethods("getRetryFactory", "getZooKeeper").createMock();
    retryFactory = EasyMock.createMock(RetryFactory.class);
    retry = EasyMock.createMock(Retry.class);

    EasyMock.expect(zrw.getZooKeeper()).andReturn(zk).anyTimes();
    EasyMock.expect(zrw.getRetryFactory()).andReturn(retryFactory).anyTimes();
    EasyMock.expect(retryFactory.create()).andReturn(retry).anyTimes();
  }

  @Test(expected = NoNodeException.class)
  public void testDeleteFailOnInitialNoNode() throws Exception {
    final String path = "/foo";
    final int version = -1;

    zk.delete(path, version);
    EasyMock.expectLastCall().andThrow(KeeperException.create(Code.NONODE));
    EasyMock.expect(retry.hasRetried()).andReturn(false);

    EasyMock.replay(zk, zrw, retryFactory, retry);

    zrw.delete(path, version);
  }

  @Test
  public void testDeleteFailOnRetry() throws Exception {
    final String path = "/foo";
    final int version = -1;

    zk.delete(path, version);
    EasyMock.expectLastCall().andThrow(KeeperException.create(Code.CONNECTIONLOSS));
    EasyMock.expect(retry.canRetry()).andReturn(true);
    retry.useRetry();
    EasyMock.expectLastCall().once();
    retry.waitForNextAttempt();
    EasyMock.expectLastCall().once();
    zk.delete(path, version);
    EasyMock.expectLastCall().andThrow(KeeperException.create(Code.NONODE));
    EasyMock.expect(retry.hasRetried()).andReturn(true);

    EasyMock.replay(zk, zrw, retryFactory, retry);

    zrw.delete(path, version);

    EasyMock.verify(zk, zrw, retryFactory, retry);
  }

  @Test(expected = SessionExpiredException.class)
  public void testMutateNodeCreationFails() throws Exception {
    final String path = "/foo";
    final byte[] value = new byte[] {0};
    final List<ACL> acls = Collections.<ACL> emptyList();
    Mutator mutator = new Mutator() {
      @Override
      public byte[] mutate(byte[] currentValue) throws Exception {
        return new byte[] {1};
      }
    };

    zk.create(path, value, acls, CreateMode.PERSISTENT);
    EasyMock.expectLastCall().andThrow(new SessionExpiredException()).once();
    EasyMock.expect(retry.canRetry()).andReturn(false);
    EasyMock.expect(retry.retriesCompleted()).andReturn(1l).once();

    EasyMock.replay(zk, zrw, retryFactory, retry);

    zrw.mutate(path, value, acls, mutator);
  }

  @Test
  public void testMutateWithBadVersion() throws Exception {
    final String path = "/foo";
    final byte[] value = new byte[] {0};
    final List<ACL> acls = Collections.<ACL> emptyList();
    final byte[] mutatedBytes = new byte[] {1};
    Mutator mutator = new Mutator() {
      @Override
      public byte[] mutate(byte[] currentValue) throws Exception {
        return mutatedBytes;
      }
    };

    Method getDataMethod = ZooReaderWriter.class.getMethod("getData", String.class, boolean.class, Stat.class);
    zrw = EasyMock.createMockBuilder(ZooReaderWriter.class).addMockedMethods("getRetryFactory", "getZooKeeper").addMockedMethod(getDataMethod).createMock();
    EasyMock.expect(zrw.getRetryFactory()).andReturn(retryFactory).anyTimes();
    EasyMock.expect(zrw.getZooKeeper()).andReturn(zk).anyTimes();

    Stat stat = new Stat();

    zk.create(path, value, acls, CreateMode.PERSISTENT);
    EasyMock.expectLastCall().andThrow(new NodeExistsException()).once();
    EasyMock.expect(zrw.getData(path, false, stat)).andReturn(new byte[] {3}).times(2);
    // BadVersionException should retry
    EasyMock.expect(zk.setData(path, mutatedBytes, 0)).andThrow(new BadVersionException());
    // Let 2nd setData succeed
    EasyMock.expect(zk.setData(path, mutatedBytes, 0)).andReturn(null);

    EasyMock.replay(zk, zrw, retryFactory, retry);

    Assert.assertArrayEquals(new byte[] {1}, zrw.mutate(path, value, acls, mutator));

    EasyMock.verify(zk, zrw, retryFactory, retry);
  }

  @Test
  public void testMutateWithRetryOnSetData() throws Exception {
    final String path = "/foo";
    final byte[] value = new byte[] {0};
    final List<ACL> acls = Collections.<ACL> emptyList();
    final byte[] mutatedBytes = new byte[] {1};
    Mutator mutator = new Mutator() {
      @Override
      public byte[] mutate(byte[] currentValue) throws Exception {
        return mutatedBytes;
      }
    };

    Method getDataMethod = ZooReaderWriter.class.getMethod("getData", String.class, boolean.class, Stat.class);
    zrw = EasyMock.createMockBuilder(ZooReaderWriter.class).addMockedMethods("getRetryFactory", "getZooKeeper").addMockedMethod(getDataMethod).createMock();
    EasyMock.expect(zrw.getRetryFactory()).andReturn(retryFactory).anyTimes();
    EasyMock.expect(zrw.getZooKeeper()).andReturn(zk).anyTimes();

    Stat stat = new Stat();

    zk.create(path, value, acls, CreateMode.PERSISTENT);
    EasyMock.expectLastCall().andThrow(new NodeExistsException()).once();
    EasyMock.expect(zrw.getData(path, false, stat)).andReturn(new byte[] {3}).times(2);
    // BadVersionException should retry
    EasyMock.expect(zk.setData(path, mutatedBytes, 0)).andThrow(new ConnectionLossException());

    EasyMock.expect(retry.canRetry()).andReturn(true);
    retry.useRetry();
    EasyMock.expectLastCall();
    retry.waitForNextAttempt();
    EasyMock.expectLastCall();
    // Let 2nd setData succeed
    EasyMock.expect(zk.setData(path, mutatedBytes, 0)).andReturn(null);

    EasyMock.replay(zk, zrw, retryFactory, retry);

    Assert.assertArrayEquals(new byte[] {1}, zrw.mutate(path, value, acls, mutator));

    EasyMock.verify(zk, zrw, retryFactory, retry);
  }
}
