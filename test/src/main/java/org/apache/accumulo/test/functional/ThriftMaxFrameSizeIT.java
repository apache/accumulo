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

import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.time.Duration;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.rpc.ThriftServerType;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TConfiguration;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MINI_CLUSTER_ONLY)
public class ThriftMaxFrameSizeIT {

  private ThriftServerType serverType;

  // use something other than TConfiguration.DEFAULT_MAX_FRAME_SIZE to make sure the override works
  // small values seem to be insufficient for Accumulo, at least for this test
  private static final int CONFIGURED_MAX_FRAME_SIZE = 32 * 1024 * 1024;

  @Nested
  class DefaultServerNestedIT extends TestMaxFrameSize {
    DefaultServerNestedIT() {
      serverType = ThriftServerType.getDefault();
    }
  }

  @Nested
  class ThreadedSelectorNestedIT extends TestMaxFrameSize {
    ThreadedSelectorNestedIT() {
      serverType = ThriftServerType.THREADED_SELECTOR;
    }
  }

  @Nested
  class CustomHsHaNestedIT extends TestMaxFrameSize {
    CustomHsHaNestedIT() {
      serverType = ThriftServerType.CUSTOM_HS_HA;
    }
  }

  @Nested
  class ThreadPoolNestedIT extends TestMaxFrameSize {
    ThreadPoolNestedIT() {
      serverType = ThriftServerType.THREADPOOL;
    }
  }

  @Nested
  class SslNestedIT extends TestMaxFrameSize {
    SslNestedIT() {
      serverType = ThriftServerType.SSL;
    }
  }

  protected abstract class TestMaxFrameSize extends ConfigurableMacBase {

    @Override
    protected Duration defaultTimeout() {
      return Duration.ofMinutes(2);
    }

    @Override
    public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
      cfg.setNumTservers(1);
      cfg.setProperty(Property.GENERAL_RPC_SERVER_TYPE, serverType.name());
      String maxFrameSizeStr = Integer.toString(CONFIGURED_MAX_FRAME_SIZE);
      cfg.setProperty(Property.GENERAL_MAX_MESSAGE_SIZE, maxFrameSizeStr);
      cfg.setProperty(Property.TSERV_MAX_MESSAGE_SIZE, maxFrameSizeStr);
      if (serverType == ThriftServerType.SSL) {
        configureForSsl(cfg,
            getSslDir(createTestDir(this.getClass().getName() + "_" + this.testName())));
      }
    }

    private void testWithSpecificSize(final int testSize) throws Exception {
      // Ingest with a value width greater than the thrift default size to verify our setting works
      // for max frame size
      try (var accumuloClient = Accumulo.newClient().from(cluster.getClientProperties()).build()) {
        String table = getUniqueNames(1)[0] + "_" + serverType.name();
        ReadWriteIT.ingest(accumuloClient, 1, 1, testSize, 0, table);
        ReadWriteIT.verify(accumuloClient, 1, 1, testSize, 0, table);
      }
    }

    // Messages bigger than the default size, but smaller than the configured max should work. This
    // means that we successfully were able to override the default values.
    @Test
    public void testFrameSizeLessThanConfiguredMax() throws Exception {
      // just use a size a little bigger than the default that would not work unless the server
      // configuration worked
      int testSize = TConfiguration.DEFAULT_MAX_FRAME_SIZE + 100;
      // just make sure it's less than what we set as the max, so we expect this to work
      assertTrue(testSize < CONFIGURED_MAX_FRAME_SIZE);
      testWithSpecificSize(testSize);
    }

    // Messages bigger than the configured size should not work.
    @Test
    public void testFrameSizeGreaterThanConfiguredMax() throws Exception {
      // ssl is weird seems to pass, at least for some values less than the default max message size
      // of 100MB; more troubleshooting might be needed to figure out how to get max message
      // configurability with ssl
      assumeFalse(this instanceof SslNestedIT);

      // just use a size a little bigger than the default that would not work with the default
      int testSize = CONFIGURED_MAX_FRAME_SIZE + 100;

      // assume it hangs forever if it doesn't finish before the timeout
      // if the timeout is too short, then we might get false negatives; in other words, the test
      // will still pass, but might not detect that the specific size unexpectedly worked
      assertThrows(AssertionError.class, () -> assertTimeoutPreemptively(Duration.ofSeconds(15),
          () -> testWithSpecificSize(testSize)));
    }

  }

}
