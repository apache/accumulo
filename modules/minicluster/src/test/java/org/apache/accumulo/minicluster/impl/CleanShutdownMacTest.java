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
package org.apache.accumulo.minicluster.impl;

import java.io.File;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.easymock.EasyMock;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 *
 */
public class CleanShutdownMacTest {

  @Rule
  public TemporaryFolder tmpDir = new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));

  @SuppressWarnings("unchecked")
  @Test
  public void testExecutorServiceShutdown() throws Exception {
    File tmp = tmpDir.newFolder();
    MiniAccumuloClusterImpl cluster = new MiniAccumuloClusterImpl(tmp, "foo");

    ExecutorService mockService = EasyMock.createMock(ExecutorService.class);
    Future<Integer> future = EasyMock.createMock(Future.class);

    cluster.setShutdownExecutor(mockService);

    EasyMock.expect(future.get()).andReturn(0).anyTimes();
    EasyMock.expect(mockService.<Integer> submit(EasyMock.anyObject(Callable.class))).andReturn(future).anyTimes();
    EasyMock.expect(mockService.shutdownNow()).andReturn(Collections.<Runnable> emptyList()).once();

    EasyMock.replay(mockService, future);

    cluster.stop();

    EasyMock.verify(mockService, future);
  }

}
