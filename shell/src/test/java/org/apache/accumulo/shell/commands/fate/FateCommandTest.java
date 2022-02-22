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
package org.apache.accumulo.shell.commands.fate;

import static org.apache.accumulo.fate.zookeeper.ServiceLock.ServiceLockPath;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.fate.FateTransactionStatus;
import org.apache.accumulo.fate.FateZooStore;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.junit.BeforeClass;
import org.junit.Test;

public class FateCommandTest {
  private static ZooReaderWriter zk;
  private static ServiceLockPath managerLockPath;
  private static ServiceLockPath tableLocksPath;

  @BeforeClass
  public static void setup() {
    zk = createMock(ZooReaderWriter.class);
    managerLockPath = createMock(ServiceLockPath.class);
    tableLocksPath = createMock(ServiceLockPath.class);
  }

  @Test
  public void testFailTx() throws Exception {
    FateZooStore zs = createMock(FateZooStore.class);
    ClientContext cc = createMock(ClientContext.class);
    String tidStr = "12345";
    long tid = Long.parseLong(tidStr, 16);
    expect(zs.getTStatus(tid)).andReturn(FateTransactionStatus.NEW).anyTimes();
    zs.reserve(tid);
    expectLastCall().once();
    zs.setStatus(tid, FateTransactionStatus.FAILED_IN_PROGRESS);
    expectLastCall().once();
    zs.unreserve(tid);
    expectLastCall().once();

    TestHelper helper = new TestHelper(zs, cc, zk, false);

    replay(zs);

    FateCommand cmd = new FateCommand();
    // require number for Tx
    assertFalse(cmd.failTx(helper, new String[] {"fail", "tx1"}));
    // fail the long configured above
    assertTrue(cmd.failTx(helper, new String[] {"fail", "12345"}));

    verify(zs);
  }

  @Test
  public void testDump() {
    FateZooStore zs = createMock(FateZooStore.class);
    Serializable ser = createMock(Serializable.class);
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

  static class TestHelper extends FateCommandHelper {

    public TestHelper(FateZooStore zs, ClientContext context, ZooReaderWriter zk,
        boolean exitOnError) {
      super(zs, context, zk, exitOnError);
      setManagerLockPath(managerLockPath);
      setTableLocksPath(tableLocksPath);
    }

    @Override
    protected boolean isManagerLockValid() throws AccumuloException {
      return true;
    }
  }
}
