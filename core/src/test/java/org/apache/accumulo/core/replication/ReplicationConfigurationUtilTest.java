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
package org.apache.accumulo.core.replication;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class ReplicationConfigurationUtilTest {

  private AccumuloConfiguration conf;

  @Before
  public void setupConfiguration() {
    Map<String,String> map = new HashMap<>();
    map.put(Property.TABLE_REPLICATION.getKey(), "true");
    conf = new ConfigurationCopy(map);
  }

  @Test
  public void rootTableExtent() {
    KeyExtent extent = new KeyExtent(RootTable.ID, null, null);
    Assert.assertFalse("The root table should never be replicated", ReplicationConfigurationUtil.isEnabled(extent, conf));
  }

  @Test
  public void metadataTableExtent() {
    KeyExtent extent = new KeyExtent(MetadataTable.ID, null, null);
    Assert.assertFalse("The metadata table should never be replicated", ReplicationConfigurationUtil.isEnabled(extent, conf));
  }

  @Test
  public void rootTableExtentEmptyConf() {
    KeyExtent extent = new KeyExtent(RootTable.ID, null, null);
    Assert.assertFalse("The root table should never be replicated",
        ReplicationConfigurationUtil.isEnabled(extent, new ConfigurationCopy(new HashMap<String,String>())));
  }

  @Test
  public void metadataTableExtentEmptyConf() {
    KeyExtent extent = new KeyExtent(MetadataTable.ID, null, null);
    Assert.assertFalse("The metadata table should never be replicated",
        ReplicationConfigurationUtil.isEnabled(extent, new ConfigurationCopy(new HashMap<String,String>())));
  }

  @Test
  public void regularTable() {
    KeyExtent extent = new KeyExtent(Table.ID.of("1"), new Text("b"), new Text("a"));
    Assert.assertTrue("Table should be replicated", ReplicationConfigurationUtil.isEnabled(extent, conf));
  }

  @Test
  public void regularNonEnabledTable() {
    KeyExtent extent = new KeyExtent(Table.ID.of("1"), new Text("b"), new Text("a"));
    Assert.assertFalse("Table should not be replicated", ReplicationConfigurationUtil.isEnabled(extent, new ConfigurationCopy(new HashMap<String,String>())));
  }
}
