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
package org.apache.accumulo.server.master;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.server.master.TabletServerLoggers;
import org.apache.accumulo.server.master.TabletServerLoggers.LoggerWatcher;
import org.junit.Test;

public class TestTabletServerLoggers {
  
  String[] theList;
  
  static class Watcher implements LoggerWatcher {
    public ArrayList<String> added = new ArrayList<String>();
    public ArrayList<String> deleted = new ArrayList<String>();
    
    @Override
    public void newLogger(String address) {
      added.add(address);
    }
    
    @Override
    public void deadLogger(String address) {
      deleted.add(address);
    }
  }
  
  Watcher watcher = new Watcher();
  
  class TabletServerLoggers_ extends TabletServerLoggers {
    
    public String[] theList = new String[0];
    
    TabletServerLoggers_() {
      super(watcher, AccumuloConfiguration.getDefaultConfiguration());
    }
    
    @Override
    public synchronized Map<String,String> getLoggersFromZooKeeper() {
      HashMap<String,String> result = new HashMap<String,String>();
      for (int i = 0; i < theList.length; i++) {
        result.put("" + i, theList[i]);
      }
      return result;
    }
    
  }
  
  @Test
  public void testScanZooKeeperForUpdates() throws Exception {
    String[] loggers = {"1.2.3.4:1234", "1.1.1.2:1234", "1.1.1.3:1234",};
    TabletServerLoggers_ lgs = new TabletServerLoggers_();
    lgs.scanZooKeeperForUpdates();
    assertEquals(lgs.getLoggersFromZooKeeper().size(), 0);
    lgs.theList = loggers;
    lgs.scanZooKeeperForUpdates();
    assertEquals(lgs.getLoggersFromZooKeeper().size(), 3);
    String[] update = {"1.2.3.4:1234"};
    lgs.theList = update;
    lgs.scanZooKeeperForUpdates();
    assertEquals(lgs.getLoggersFromZooKeeper().size(), 1);
    assertEquals(watcher.added.size(), 3);
    assertEquals(watcher.deleted.size(), 2);
    assertEquals(watcher.deleted, Arrays.asList("1.1.1.3:1234", "1.1.1.2:1234"));
  }
  
}
