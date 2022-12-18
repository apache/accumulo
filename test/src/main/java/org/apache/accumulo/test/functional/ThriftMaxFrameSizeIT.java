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

import java.time.Duration;

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

public class ThriftMaxFrameSizeIT extends AccumuloClusterHarness {

  private ThriftServerType serverType;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.GENERAL_RPC_SERVER_TYPE, serverType.name());
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
  }

  @Nested
  class TestThreadedSelector extends TestMaxFrameSize {
    TestThreadedSelector() {
      serverType = ThriftServerType.THREADED_SELECTOR;
    }
  }

  @Nested
  class TestCustomHsHa extends TestMaxFrameSize {
    TestCustomHsHa() {
      serverType = ThriftServerType.CUSTOM_HS_HA;
    }
  }

  @Nested
  class TestThreadPool extends TestMaxFrameSize {
    TestThreadPool() {
      serverType = ThriftServerType.THREADPOOL;
    }
  }

  @Nested
  class TestSsl extends TestMaxFrameSize {
    TestSsl() {
      serverType = ThriftServerType.THREADPOOL;
    }
  }

  protected abstract class TestMaxFrameSize {

    @Test
    public void testMaxFrameSizeLargerThanDefault() throws Exception {

      // Ingest with a value width greater than the thrift default size to verify our setting works
      // for max frame wize
      try (AccumuloClient accumuloClient = Accumulo.newClient().from(getClientProps()).build()) {
        String table = getUniqueNames(1)[0];
        ReadWriteIT.ingest(accumuloClient, 1, 1, TConfiguration.DEFAULT_MAX_FRAME_SIZE + 1, 0,
            table);
        ReadWriteIT.verify(accumuloClient, 1, 1, TConfiguration.DEFAULT_MAX_FRAME_SIZE + 1, 0,
            table);
      }
    }
  }

}
