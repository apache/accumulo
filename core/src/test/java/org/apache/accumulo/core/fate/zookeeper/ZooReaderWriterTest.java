/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.fate.zookeeper;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter.Mutator;
import org.apache.accumulo.core.util.Retry;
import org.apache.accumulo.core.util.Retry.RetryFactory;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.BadVersionException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ZooReaderWriterTest {

  private ZooReaderWriter zrw;
  private ZooSession zk;
  private RetryFactory retryFactory;
  private Retry retry;

  @BeforeEach
  public void setup() {
    zk = createMock(ZooSession.class);
    retryFactory = createMock(RetryFactory.class);
    retry = createMock(Retry.class);
    expect(retryFactory.createRetry()).andReturn(retry).anyTimes();
    zrw = new ZooReaderWriter(zk) {
      @Override
      protected RetryFactory getRetryFactory() {
        return retryFactory;
      }
    };
    expect(zk.asReaderWriter()).andReturn(zrw).anyTimes();
  }

  @Test
  public void testDeleteSucceedOnInitialNoNode() throws Exception {
    final String path = "/foo";

    zk.delete(path, -1);
    expectLastCall().andThrow(KeeperException.create(Code.NONODE));

    replay(zk, retryFactory, retry);

    zrw.delete(path);

    verify(zk, retryFactory, retry);
  }

  @Test
  public void testDeleteSucceedOnRetry() throws Exception {
    final String path = "/foo";

    zk.delete(path, -1);
    expectLastCall().andThrow(KeeperException.create(Code.CONNECTIONLOSS));
    expect(retry.canRetry()).andReturn(true);
    retry.useRetry();
    expectLastCall().once();
    retry.waitForNextAttempt(anyObject(), anyString());
    expectLastCall().once();
    zk.delete(path, -1);
    expectLastCall().andThrow(KeeperException.create(Code.NONODE));

    replay(zk, retryFactory, retry);

    zrw.delete(path);

    verify(zk, retryFactory, retry);
  }

  @Test
  public void testMutateNodeCreationFails() throws Exception {
    final String path = "/foo";
    final byte[] value = {0};
    Mutator mutator = currentValue -> new byte[] {1};

    zk.create(path, value, ZooUtil.PUBLIC, CreateMode.PERSISTENT);
    expectLastCall().andThrow(new SessionExpiredException()).once();
    expect(retry.canRetry()).andReturn(false);
    expect(retry.retriesCompleted()).andReturn(1L).once();

    replay(zk, retryFactory, retry);

    assertThrows(SessionExpiredException.class, () -> zrw.mutateOrCreate(path, value, mutator));

    verify(zk, retryFactory, retry);
  }

  @Test
  public void testMutateWithBadVersion() throws Exception {
    final String path = "/foo";
    final byte[] value = {0};
    final byte[] mutatedBytes = {1};
    Mutator mutator = currentValue -> mutatedBytes;

    Stat stat = new Stat();

    zk.create(path, value, ZooUtil.PUBLIC, CreateMode.PERSISTENT);
    expectLastCall().andThrow(new NodeExistsException()).once();
    expect(zk.getData(path, null, stat)).andReturn(new byte[] {3}).times(2);
    // BadVersionException should retry
    expect(zk.setData(path, mutatedBytes, 0)).andThrow(new BadVersionException());
    // Let 2nd setData succeed
    expect(zk.setData(path, mutatedBytes, 0)).andReturn(null);

    retry.waitForNextAttempt(anyObject(), anyObject());
    expectLastCall().once();

    replay(zk, retryFactory, retry);

    assertArrayEquals(new byte[] {1}, zrw.mutateOrCreate(path, value, mutator));

    verify(zk, retryFactory, retry);
  }

  @Test
  public void testMutateWithRetryOnSetData() throws Exception {
    final String path = "/foo";
    final byte[] value = {0};
    final byte[] mutatedBytes = {1};
    Mutator mutator = currentValue -> mutatedBytes;

    Stat stat = new Stat();

    zk.create(path, value, ZooUtil.PUBLIC, CreateMode.PERSISTENT);
    expectLastCall().andThrow(new NodeExistsException()).once();
    expect(zk.getData(path, null, stat)).andReturn(new byte[] {3}).times(2);
    // transient connection loss should retry
    expect(zk.setData(path, mutatedBytes, 0)).andThrow(new ConnectionLossException());

    expect(retry.canRetry()).andReturn(true);
    retry.useRetry();
    expectLastCall();
    retry.waitForNextAttempt(anyObject(), anyString());
    expectLastCall();
    // Let 2nd setData succeed
    expect(zk.setData(path, mutatedBytes, 0)).andReturn(null);

    replay(zk, retryFactory, retry);

    assertArrayEquals(new byte[] {1}, zrw.mutateOrCreate(path, value, mutator));

    verify(zk, retryFactory, retry);
  }
}
