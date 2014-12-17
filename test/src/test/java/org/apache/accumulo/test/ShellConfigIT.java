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
package org.apache.accumulo.test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.ShellServerIT.TestShell;
import org.apache.accumulo.test.functional.ConfigurableMacIT;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class ShellConfigIT extends ConfigurableMacIT {
  @Override
  public int defaultTimeoutSeconds() {
    return 30;
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.CRYPTO_BLOCK_STREAM_SIZE, "7K");
  }

  @Test
  public void experimentalPropTest() throws Exception {
    // ensure experimental props do not show up in config output unless set

    TestShell ts = new TestShell(ROOT_PASSWORD, getCluster().getInstanceName(), getCluster().getZooKeepers(), getCluster().getConfig().getClientConfFile()
        .getAbsolutePath());

    assertTrue(Property.CRYPTO_BLOCK_STREAM_SIZE.isExperimental());
    assertTrue(Property.CRYPTO_CIPHER_ALGORITHM_NAME.isExperimental());

    String configOutput = ts.exec("config");

    assertTrue(configOutput.contains(Property.CRYPTO_BLOCK_STREAM_SIZE.getKey()));
    assertFalse(configOutput.contains(Property.CRYPTO_CIPHER_ALGORITHM_NAME.getKey()));
  }
}
