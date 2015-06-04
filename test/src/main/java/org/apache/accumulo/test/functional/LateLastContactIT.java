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
package org.apache.accumulo.test.functional;

import static org.junit.Assert.assertEquals;

import java.util.Collections;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * Fake the "tablet stops talking but holds its lock" problem we see when hard drives and NFS fail. Start a ZombieTServer, and see that master stops it.
 */
public class LateLastContactIT extends ConfigurableMacBase {

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setSiteConfig(Collections.singletonMap(Property.GENERAL_RPC_TIMEOUT.getKey(), "2s"));
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 60;
  }

  @Test
  public void test() throws Exception {
    Process zombie = cluster.exec(ZombieTServer.class);
    assertEquals(0, zombie.waitFor());
  }

}
