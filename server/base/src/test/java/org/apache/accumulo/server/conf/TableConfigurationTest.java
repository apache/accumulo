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
package org.apache.accumulo.server.conf;

import org.apache.accumulo.core.client.Instance;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import org.easymock.Capture;
import static org.easymock.EasyMock.*;

public class TableConfigurationTest {
  private static final String INSTANCE_ID = "instanceId";
  private static final String TABLE = "table";
  private Instance instance;
  private NamespaceConfiguration nsconf;
  private TableConfiguration tconf;

  @Before
  public void setUp() throws Exception {
    instance = createMock(Instance.class);
    nsconf = createMock(NamespaceConfiguration.class);
    tconf = new TableConfiguration(INSTANCE_ID, TABLE, nsconf);
  }

  @Test
  public void testGetters() {
    assertEquals(TABLE, tconf.getTableId());
    assertSame(nsconf, tconf.getParentConfiguration());
  }
}
