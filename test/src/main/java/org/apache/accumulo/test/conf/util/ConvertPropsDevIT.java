/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test.conf.util;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.server.conf.store.PropCacheKey;
import org.apache.accumulo.server.conf.store.impl.ZooPropStore;
import org.apache.accumulo.server.conf.util.ConfigConverter;
import org.apache.accumulo.server.conf.util.ConfigPropertyPrinter;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConvertPropsDevIT extends AccumuloClusterHarness {

  private static final Logger log = LoggerFactory.getLogger(ConvertPropsDevIT.class);

  private AccumuloClient client;

  @AfterClass
  public static void cleanup() {

  }

  @Before
  public void setup() {
    client = Accumulo.newClient().from(getClientProps()).build();
  }

  @After
  public void closeClient() {
    client.close();
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  @Test
  public void go() throws Exception {

    // system
    client.instanceOperations().setProperty(Property.TABLE_SPLIT_THRESHOLD.getKey(), "111M");

    // default ns
    client.namespaceOperations().setProperty("", Property.TABLE_SPLIT_THRESHOLD.getKey(), "321M");

    // user ns
    client.namespaceOperations().create("ns1");
    client.namespaceOperations().setProperty("ns1", Property.TABLE_SPLIT_THRESHOLD.getKey(),
        "222M");
    client.tableOperations().create("ns1.tbl1");
    client.tableOperations().create("ns1.tbl2");
    client.tableOperations().setProperty("ns1.tbl2", Property.TABLE_SPLIT_THRESHOLD.getKey(),
        "333M");
    client.tableOperations().create("tbl3");
    client.tableOperations().setProperty("tbl3", Property.TABLE_SPLIT_THRESHOLD.getKey(), "444M");
    client.tableOperations().create("tbl4");

    ConfigConverter upgrade = new ConfigConverter(getServerContext());
    upgrade.convertSys();
    upgrade.convertNamespace();
    upgrade.convertTables();

    ZooReaderWriter zrw = getServerContext().getZooReaderWriter();
    InstanceId IID = getServerContext().getInstanceID();

    log.info("\n\nIID: {}\n", IID);

    var codec = ZooPropStore.getCodec();

    Stat stat = new Stat();
    byte[] bytes = zrw.getData(PropCacheKey.forSystem(getServerContext()).getPath(), stat);
    log.info("versioned props: {}", codec.fromBytes(stat.getVersion(), bytes));

    ConfigPropertyPrinter printer = new ConfigPropertyPrinter();
    printer.print(getServerContext(), "./target/test.out", false);
  }
}
