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
package org.apache.accumulo.server.watcher;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.junit.Before;
import org.junit.Test;

public class MonitorLog4jWatcherTest {
  private static final String INSTANCE = "instance";
  private static final String FILENAME = "something_logger.xml";

  private MonitorLog4jWatcher w;

  @Before
  public void setUp() throws Exception {
    w = new MonitorLog4jWatcher(INSTANCE, FILENAME);
  }

  @Test
  public void testGetters() {
    assertFalse(w.isUsingProperties());
    assertEquals(ZooUtil.getRoot(INSTANCE) + Constants.ZMONITOR_LOG4J_ADDR, w.getPath());
  }

  @Test
  public void testPropertyDetection() {
    w = new MonitorLog4jWatcher(INSTANCE, FILENAME + ".properties");
    assertTrue(w.isUsingProperties());
  }
}
