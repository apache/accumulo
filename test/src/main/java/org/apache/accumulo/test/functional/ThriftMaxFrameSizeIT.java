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
package org.apache.accumulo.test.functional;

import static org.apache.accumulo.test.functional.ConfigurableMacBase.configureForSsl;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.rpc.ThriftServerType;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TConfiguration;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

public class ThriftMaxFrameSizeIT extends AccumuloClusterHarness {

  private ThriftServerType serverType;

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
    cfg.setProperty(Property.GENERAL_RPC_SERVER_TYPE, serverType.name());
    cfg.setProperty(Property.GENERAL_MAX_MESSAGE_SIZE,
        Integer.toString(TConfiguration.DEFAULT_MAX_FRAME_SIZE));
    cfg.setProperty(Property.TSERV_MAX_MESSAGE_SIZE,
        Integer.toString(TConfiguration.DEFAULT_MAX_FRAME_SIZE));
    if (serverType == ThriftServerType.SSL) {
      configureForSsl(cfg,
          getSslDir(createTestDir(this.getClass().getName() + "_" + this.testName())));
    }
  }

  @Nested
  class TestDefault extends TestMaxFrameSize {
    TestDefault() {
      serverType = ThriftServerType.getDefault();
    }

    @Test
    public void testDefaultServerFrameSize() throws Exception {
      AtomicBoolean succeeded = new AtomicBoolean(false);
      assertThrows(AssertionFailedError.class,
          () -> assertTimeoutPreemptively(Duration.ofSeconds(30),
              () -> testMaxFrameSizeLargerThanDefault(succeeded)));
      assertFalse(succeeded.get());
    }
  }

  @Nested
  class TestThreadedSelector extends TestMaxFrameSize {
    TestThreadedSelector() {
      serverType = ThriftServerType.THREADED_SELECTOR;
    }

    @Test
    public void testThreadedSelectorServerFrameSize() throws Exception {
      AtomicBoolean succeeded = new AtomicBoolean(false);
      assertThrows(AssertionFailedError.class,
          () -> assertTimeoutPreemptively(Duration.ofSeconds(30),
              () -> testMaxFrameSizeLargerThanDefault(succeeded)));
      assertFalse(succeeded.get());
    }
  }

  @Nested
  class TestCustomHsHa extends TestMaxFrameSize {
    TestCustomHsHa() {
      serverType = ThriftServerType.CUSTOM_HS_HA;
    }

    @Test
    public void testCustomHsHaServerFrameSize() throws Exception {
      AtomicBoolean succeeded = new AtomicBoolean(false);
      assertThrows(AssertionFailedError.class,
          () -> assertTimeoutPreemptively(Duration.ofSeconds(30),
              () -> testMaxFrameSizeLargerThanDefault(succeeded)));
      assertFalse(succeeded.get());
    }
  }

  @Nested
  class TestThreadPool extends TestMaxFrameSize {
    TestThreadPool() {
      serverType = ThriftServerType.THREADPOOL;
    }

    @Test
    public void testThreadPoolServerFrameSize() throws Exception {
      AtomicBoolean succeeded = new AtomicBoolean(false);
      assertThrows(AssertionFailedError.class,
          () -> assertTimeoutPreemptively(Duration.ofSeconds(30),
              () -> testMaxFrameSizeLargerThanDefault(succeeded)));
      assertFalse(succeeded.get());
    }
  }

  @Nested
  class TestSsl extends TestMaxFrameSize {
    TestSsl() {
      serverType = ThriftServerType.SSL;
    }

    @Test
    public void testSslServerFrameSize() throws Exception {
      AtomicBoolean succeeded = new AtomicBoolean(false);
      testMaxFrameSizeLargerThanDefault(succeeded);
      assertTrue(succeeded.get());
    }
  }

  protected abstract class TestMaxFrameSize {

    public void testMaxFrameSizeLargerThanDefault(AtomicBoolean success) throws Exception {

      int maxSize = TConfiguration.DEFAULT_MAX_FRAME_SIZE;
      // make sure we go even bigger than that
      int ourSize = maxSize * 2;

      // Ingest with a value width greater than the thrift default size to verify our setting works
      // for max frame size
      try (AccumuloClient accumuloClient = Accumulo.newClient().from(getClientProps()).build()) {
        String table = getUniqueNames(1)[0] + serverType.name();
        ReadWriteIT.ingest(accumuloClient, 1, 1, ourSize, 0, table);
        ReadWriteIT.verify(accumuloClient, 1, 1, ourSize, 0, table);
      }
      success.set(true);
    }
  }

}
