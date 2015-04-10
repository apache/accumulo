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
package org.apache.accumulo.tserver;

import static org.easymock.EasyMock.createMock;
import static org.junit.Assert.assertEquals;

import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.tserver.TabletServerResourceManager.TabletResourceManager;
import org.junit.Before;
import org.junit.Test;

public class TabletResourceManagerTest {
  private TabletServerResourceManager tsrm;
  private TabletResourceManager trm;
  private KeyExtent extent;
  private TableConfiguration conf;

  @Before
  public void setUp() throws Exception {
    tsrm = createMock(TabletServerResourceManager.class);
    extent = createMock(KeyExtent.class);
    conf = createMock(TableConfiguration.class);
    trm = tsrm.new TabletResourceManager(extent, conf);
  }

  @Test
  public void testConstruction() {
    assertEquals(extent, trm.getExtent());
    assertEquals(conf, trm.getTableConfiguration());
  }
}
