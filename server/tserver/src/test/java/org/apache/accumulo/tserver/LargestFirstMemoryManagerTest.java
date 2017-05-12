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

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.server.conf.NamespaceConfiguration;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.tabletserver.LargestFirstMemoryManager;
import org.apache.accumulo.server.tabletserver.MemoryManagementActions;
import org.apache.accumulo.server.tabletserver.TabletState;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

public class LargestFirstMemoryManagerTest {

  private static final long ZERO = System.currentTimeMillis();
  private static final long LATER = ZERO + 20 * 60 * 1000;
  private static final long ONE_GIG = 1024 * 1024 * 1024;
  private static final long HALF_GIG = ONE_GIG / 2;
  private static final long QGIG = ONE_GIG / 4;
  private static final long ONE_MINUTE = 60 * 1000;

  private Instance inst;

  @Before
  public void mockInstance() {
    inst = EasyMock.createMock(Instance.class);
  }

  @Test
  public void test() throws Exception {
    LargestFirstMemoryManagerUnderTest mgr = new LargestFirstMemoryManagerUnderTest();
    ServerConfiguration config = new ServerConfiguration() {
      ServerConfigurationFactory delegate = new ServerConfigurationFactory(inst);

      @Override
      public AccumuloConfiguration getSystemConfiguration() {
        SiteConfiguration conf = SiteConfiguration.getInstance();
        conf.set(Property.TSERV_MAXMEM, "1g");
        return conf;
      }

      @Override
      public TableConfiguration getTableConfiguration(String tableId) {
        return delegate.getTableConfiguration(tableId);
      }

      @Override
      public NamespaceConfiguration getNamespaceConfiguration(String namespaceId) {
        return delegate.getNamespaceConfiguration(namespaceId);
      }

      @Override
      public Instance getInstance() {
        return delegate.getInstance();
      }
    };
    mgr.init(config);
    MemoryManagementActions result;
    // nothing to do
    result = mgr.getMemoryManagementActions(tablets(t(k("x"), ZERO, 1000, 0), t(k("y"), ZERO, 2000, 0)));
    assertEquals(0, result.tabletsToMinorCompact.size());
    // one tablet is really big
    result = mgr.getMemoryManagementActions(tablets(t(k("x"), ZERO, ONE_GIG, 0), t(k("y"), ZERO, 2000, 0)));
    assertEquals(1, result.tabletsToMinorCompact.size());
    assertEquals(k("x"), result.tabletsToMinorCompact.get(0));
    // one tablet is idle
    mgr.currentTime = LATER;
    result = mgr.getMemoryManagementActions(tablets(t(k("x"), ZERO, 1001, 0), t(k("y"), LATER, 2000, 0)));
    assertEquals(1, result.tabletsToMinorCompact.size());
    assertEquals(k("x"), result.tabletsToMinorCompact.get(0));
    // one tablet is idle, but one is really big
    result = mgr.getMemoryManagementActions(tablets(t(k("x"), ZERO, 1001, 0), t(k("y"), LATER, ONE_GIG, 0)));
    assertEquals(1, result.tabletsToMinorCompact.size());
    assertEquals(k("y"), result.tabletsToMinorCompact.get(0));
    // lots of work to do
    mgr = new LargestFirstMemoryManagerUnderTest();
    mgr.init(config);
    result = mgr.getMemoryManagementActions(tablets(t(k("a"), ZERO, HALF_GIG, 0), t(k("b"), ZERO, HALF_GIG + 1, 0), t(k("c"), ZERO, HALF_GIG + 2, 0),
        t(k("d"), ZERO, HALF_GIG + 3, 0), t(k("e"), ZERO, HALF_GIG + 4, 0), t(k("f"), ZERO, HALF_GIG + 5, 0), t(k("g"), ZERO, HALF_GIG + 6, 0),
        t(k("h"), ZERO, HALF_GIG + 7, 0), t(k("i"), ZERO, HALF_GIG + 8, 0)));
    assertEquals(2, result.tabletsToMinorCompact.size());
    assertEquals(k("i"), result.tabletsToMinorCompact.get(0));
    assertEquals(k("h"), result.tabletsToMinorCompact.get(1));
    // one finished, one in progress, one filled up
    mgr = new LargestFirstMemoryManagerUnderTest();
    mgr.init(config);
    result = mgr.getMemoryManagementActions(tablets(t(k("a"), ZERO, HALF_GIG, 0), t(k("b"), ZERO, HALF_GIG + 1, 0), t(k("c"), ZERO, HALF_GIG + 2, 0),
        t(k("d"), ZERO, HALF_GIG + 3, 0), t(k("e"), ZERO, HALF_GIG + 4, 0), t(k("f"), ZERO, HALF_GIG + 5, 0), t(k("g"), ZERO, ONE_GIG, 0),
        t(k("h"), ZERO, 0, HALF_GIG + 7), t(k("i"), ZERO, 0, 0)));
    assertEquals(1, result.tabletsToMinorCompact.size());
    assertEquals(k("g"), result.tabletsToMinorCompact.get(0));
    // memory is very full, lots of candidates
    result = mgr.getMemoryManagementActions(tablets(t(k("a"), ZERO, HALF_GIG, 0), t(k("b"), ZERO, ONE_GIG + 1, 0), t(k("c"), ZERO, ONE_GIG + 2, 0),
        t(k("d"), ZERO, ONE_GIG + 3, 0), t(k("e"), ZERO, ONE_GIG + 4, 0), t(k("f"), ZERO, ONE_GIG + 5, 0), t(k("g"), ZERO, ONE_GIG + 6, 0),
        t(k("h"), ZERO, 0, 0), t(k("i"), ZERO, 0, 0)));
    assertEquals(2, result.tabletsToMinorCompact.size());
    assertEquals(k("g"), result.tabletsToMinorCompact.get(0));
    assertEquals(k("f"), result.tabletsToMinorCompact.get(1));
    // only have two compactors, still busy
    result = mgr.getMemoryManagementActions(tablets(t(k("a"), ZERO, HALF_GIG, 0), t(k("b"), ZERO, ONE_GIG + 1, 0), t(k("c"), ZERO, ONE_GIG + 2, 0),
        t(k("d"), ZERO, ONE_GIG + 3, 0), t(k("e"), ZERO, ONE_GIG + 4, 0), t(k("f"), ZERO, ONE_GIG, ONE_GIG + 5), t(k("g"), ZERO, ONE_GIG, ONE_GIG + 6),
        t(k("h"), ZERO, 0, 0), t(k("i"), ZERO, 0, 0)));
    assertEquals(0, result.tabletsToMinorCompact.size());
    // finished one
    result = mgr.getMemoryManagementActions(tablets(t(k("a"), ZERO, HALF_GIG, 0), t(k("b"), ZERO, ONE_GIG + 1, 0), t(k("c"), ZERO, ONE_GIG + 2, 0),
        t(k("d"), ZERO, ONE_GIG + 3, 0), t(k("e"), ZERO, ONE_GIG + 4, 0), t(k("f"), ZERO, ONE_GIG, ONE_GIG + 5), t(k("g"), ZERO, ONE_GIG, 0),
        t(k("h"), ZERO, 0, 0), t(k("i"), ZERO, 0, 0)));
    assertEquals(1, result.tabletsToMinorCompact.size());
    assertEquals(k("e"), result.tabletsToMinorCompact.get(0));

    // many are running: do nothing
    mgr = new LargestFirstMemoryManagerUnderTest();
    mgr.init(config);
    result = mgr.getMemoryManagementActions(tablets(t(k("a"), ZERO, HALF_GIG, 0), t(k("b"), ZERO, HALF_GIG + 1, 0), t(k("c"), ZERO, HALF_GIG + 2, 0),
        t(k("d"), ZERO, 0, HALF_GIG), t(k("e"), ZERO, 0, HALF_GIG), t(k("f"), ZERO, 0, HALF_GIG), t(k("g"), ZERO, 0, HALF_GIG), t(k("i"), ZERO, 0, HALF_GIG),
        t(k("j"), ZERO, 0, HALF_GIG), t(k("k"), ZERO, 0, HALF_GIG), t(k("l"), ZERO, 0, HALF_GIG), t(k("m"), ZERO, 0, HALF_GIG)));
    assertEquals(0, result.tabletsToMinorCompact.size());

    // observe adjustment:
    mgr = new LargestFirstMemoryManagerUnderTest();
    mgr.init(config);
    // compact the largest
    result = mgr.getMemoryManagementActions(tablets(t(k("a"), ZERO, QGIG, 0), t(k("b"), ZERO, QGIG + 1, 0), t(k("c"), ZERO, QGIG + 2, 0)));
    assertEquals(1, result.tabletsToMinorCompact.size());
    assertEquals(k("c"), result.tabletsToMinorCompact.get(0));
    // show that it is compacting... do nothing
    result = mgr.getMemoryManagementActions(tablets(t(k("a"), ZERO, QGIG, 0), t(k("b"), ZERO, QGIG + 1, 0), t(k("c"), ZERO, 0, QGIG + 2)));
    assertEquals(0, result.tabletsToMinorCompact.size());
    // not going to bother compacting any more
    mgr.currentTime += ONE_MINUTE;
    result = mgr.getMemoryManagementActions(tablets(t(k("a"), ZERO, QGIG, 0), t(k("b"), ZERO, QGIG + 1, 0), t(k("c"), ZERO, 0, QGIG + 2)));
    assertEquals(0, result.tabletsToMinorCompact.size());
    // now do nothing
    mgr.currentTime += ONE_MINUTE;
    result = mgr.getMemoryManagementActions(tablets(t(k("a"), ZERO, QGIG, 0), t(k("b"), ZERO, 0, 0), t(k("c"), ZERO, 0, 0)));
    assertEquals(0, result.tabletsToMinorCompact.size());
    // on no! more data, this time we compact because we've adjusted
    mgr.currentTime += ONE_MINUTE;
    result = mgr.getMemoryManagementActions(tablets(t(k("a"), ZERO, QGIG, 0), t(k("b"), ZERO, QGIG + 1, 0), t(k("c"), ZERO, 0, 0)));
    assertEquals(1, result.tabletsToMinorCompact.size());
    assertEquals(k("b"), result.tabletsToMinorCompact.get(0));
  }

  @Test
  public void testDeletedTable() throws Exception {
    final String deletedTableId = "1";
    Function<String,Boolean> existenceCheck = tableId -> !deletedTableId.equals(tableId);
    LargestFirstMemoryManagerWithExistenceCheck mgr = new LargestFirstMemoryManagerWithExistenceCheck(existenceCheck);
    ServerConfiguration config = new ServerConfiguration() {
      ServerConfigurationFactory delegate = new ServerConfigurationFactory(inst);

      @Override
      public AccumuloConfiguration getSystemConfiguration() {
        return DefaultConfiguration.getInstance();
      }

      @Override
      public TableConfiguration getTableConfiguration(String tableId) {
        return delegate.getTableConfiguration(tableId);
      }

      @Override
      public NamespaceConfiguration getNamespaceConfiguration(String namespaceId) {
        return delegate.getNamespaceConfiguration(namespaceId);
      }

      @Override
      public Instance getInstance() {
        return delegate.getInstance();
      }
    };
    mgr.init(config);
    MemoryManagementActions result;
    // one tablet is really big and the other is for a nonexistent table
    KeyExtent extent = new KeyExtent("2", new Text("j"), null);
    result = mgr.getMemoryManagementActions(tablets(t(extent, ZERO, ONE_GIG, 0), t(k("j"), ZERO, ONE_GIG, 0)));
    assertEquals(1, result.tabletsToMinorCompact.size());
    assertEquals(extent, result.tabletsToMinorCompact.get(0));
  }

  private static class LargestFirstMemoryManagerUnderTest extends LargestFirstMemoryManager {

    public long currentTime = ZERO;

    @Override
    protected long currentTimeMillis() {
      return currentTime;
    }

    @Override
    protected long getMinCIdleThreshold(KeyExtent extent) {
      return 15 * 60 * 1000;
    }

    @Override
    protected boolean tableExists(Instance instance, String tableId) {
      return true;
    }
  }

  private static class LargestFirstMemoryManagerWithExistenceCheck extends LargestFirstMemoryManagerUnderTest {

    Function<String,Boolean> existenceCheck;

    public LargestFirstMemoryManagerWithExistenceCheck(Function<String,Boolean> existenceCheck) {
      super();
      this.existenceCheck = existenceCheck;
    }

    @Override
    protected boolean tableExists(Instance instance, String tableId) {
      return existenceCheck.apply(tableId);
    }
  }

  private static KeyExtent k(String endRow) {
    return new KeyExtent("1", new Text(endRow), null);
  }

  private static class TestTabletState implements TabletState {

    private final KeyExtent extent;
    private final long lastCommit;
    private final long memSize;
    private final long compactingSize;

    TestTabletState(KeyExtent extent, long commit, long memsize, long compactingTableSize) {
      this.extent = extent;
      this.lastCommit = commit;
      this.memSize = memsize;
      this.compactingSize = compactingTableSize;
    }

    @Override
    public KeyExtent getExtent() {
      return extent;
    }

    @Override
    public long getLastCommitTime() {
      return lastCommit;
    }

    @Override
    public long getMemTableSize() {
      return memSize;
    }

    @Override
    public long getMinorCompactingMemTableSize() {
      return compactingSize;
    }

  }

  private TabletState t(KeyExtent ke, long lastCommit, long memSize, long compactingSize) {
    return new TestTabletState(ke, lastCommit, memSize, compactingSize);
  }

  private static List<TabletState> tablets(TabletState... states) {
    return Arrays.asList(states);
  }

}
