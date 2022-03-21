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
package org.apache.accumulo.core.spi.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class DefaultScanServerDispatcherTest {

  static class InitParams implements ScanServerDispatcher.InitParameters {

    private final Map<String,String> opts;
    private final Set<String> scanServers;

    InitParams(Map<String,String> opts, Set<String> scanServers) {
      this.opts = opts;
      this.scanServers = scanServers;
    }

    InitParams(Set<String> scanServers) {
      this.opts = Map.of();
      this.scanServers = scanServers;
    }

    @Override
    public Map<String,String> getOptions() {
      return opts;
    }

    @Override
    public ServiceEnvironment getServiceEnv() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Supplier<Set<String>> getScanServers() {
      return () -> scanServers;
    }
  }

  static class DaParams implements ScanServerDispatcher.DispatcherParameters {

    private final Collection<TabletId> tablets;
    private final Map<TabletId,Collection<ScanServerDispatcher.ScanAttempt>> attempts;

    DaParams(Collection<TabletId> tablets,
        Map<TabletId,Collection<ScanServerDispatcher.ScanAttempt>> attempts) {
      this.tablets = tablets;
      this.attempts = attempts;
    }

    DaParams(TabletId tablet) {
      this.tablets = Set.of(tablet);
      this.attempts = Map.of();
    }

    @Override
    public Collection<TabletId> getTablets() {
      return tablets;
    }

    @Override
    public Collection<? extends ScanServerDispatcher.ScanAttempt> getAttempts(TabletId tabletId) {
      return attempts.getOrDefault(tabletId, Set.of());
    }
  }

  TabletId nti(String tableId, String endRow) {
    return new TabletIdImpl(new KeyExtent(TableId.of(tableId), new Text(endRow), null));
  }

  @Test
  public void testBasic() {
    DefaultScanServerDispatcher dispatcher = new DefaultScanServerDispatcher();
    dispatcher.init(new InitParams(
        Set.of("ss1:1", "ss2:2", "ss3:3", "ss4:4", "ss5:5", "ss6:6", "ss7:7", "ss8:8")));

    Set<String> servers = new HashSet<>();

    for (int i = 0; i < 100; i++) {
      var tabletId = nti("1", "m");

      ScanServerDispatcher.Actions actions = dispatcher.determineActions(new DaParams(tabletId));

      servers.add(actions.getScanServer(tabletId));
    }

    assertEquals(3, servers.size());
  }

}
