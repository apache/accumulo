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
package org.apache.accumulo.shell.commands;

import static org.apache.accumulo.fate.zookeeper.ServiceLock.ServiceLockPath;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.apache.accumulo.fate.AdminUtil;
import org.apache.accumulo.fate.ReadOnlyRepo;
import org.apache.accumulo.fate.ReadOnlyTStore;
import org.apache.accumulo.fate.ZooStore;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class FateCommandTest {
  private static ZooReaderWriter zk;
  private static ServiceLockPath managerLockPath;

  @BeforeAll
  public static void setup() {
    zk = createMock(ZooReaderWriter.class);
    managerLockPath = createMock(ServiceLockPath.class);
  }

  @Test
  public void testFailTx() throws Exception {
    ZooStore<FateCommand> zs = createMock(ZooStore.class);
    String tidStr = "12345";
    long tid = Long.parseLong(tidStr, 16);
    expect(zs.getStatus(tid)).andReturn(ReadOnlyTStore.TStatus.NEW).anyTimes();
    zs.reserve(tid);
    expectLastCall().once();
    zs.setStatus(tid, ReadOnlyTStore.TStatus.FAILED_IN_PROGRESS);
    expectLastCall().once();
    zs.unreserve(tid, 0);
    expectLastCall().once();

    TestHelper helper = new TestHelper(true);

    replay(zs);

    FateCommand cmd = new FateCommand();
    // require number for Tx
    assertFalse(cmd.failTx(helper, zs, zk, managerLockPath, new String[] {"fail", "tx1"}));
    // fail the long configured above
    assertTrue(cmd.failTx(helper, zs, zk, managerLockPath, new String[] {"fail", "12345"}));

    verify(zs);
  }

  @Test
  public void testDump() {
    ZooStore<FateCommand> zs = createMock(ZooStore.class);
    ReadOnlyRepo<FateCommand> ser = createMock(ReadOnlyRepo.class);
    long tid1 = Long.parseLong("12345", 16);
    long tid2 = Long.parseLong("23456", 16);
    expect(zs.getStack(tid1)).andReturn(List.of(ser)).once();
    expect(zs.getStack(tid2)).andReturn(List.of(ser)).once();

    replay(zs);

    FateCommand cmd = new FateCommand();

    var args = new String[] {"dump", "12345", "23456"};
    var output = cmd.dumpTx(zs, args);
    System.out.println(output);
    assertTrue(output.contains("0000000000012345"));
    assertTrue(output.contains("0000000000023456"));

    verify(zs);
  }

  static class TestHelper extends AdminUtil<FateCommand> {

    public TestHelper(boolean exitOnError) {
      super(exitOnError);
    }

    @Override
    public boolean checkGlobalLock(ZooReaderWriter zk, ServiceLockPath zLockManagerPath) {
      return true;
    }
  }
}
