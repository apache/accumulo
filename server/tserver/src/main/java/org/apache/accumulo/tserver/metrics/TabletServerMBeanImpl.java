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
package org.apache.accumulo.tserver.metrics;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.accumulo.server.metrics.AbstractMetricsImpl;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.accumulo.tserver.tablet.Tablet;

public class TabletServerMBeanImpl extends AbstractMetricsImpl implements TabletServerMBean {

  private static final String METRICS_PREFIX = "tserver";
  private static ObjectName OBJECT_NAME = null;

  final TabletServer server;
  
  public TabletServerMBeanImpl(TabletServer server) throws MalformedObjectNameException {
    this.server = server;
    OBJECT_NAME = new ObjectName("accumulo.server.metrics:service=TServerInfo,name=TabletServerMBean,instance=" + Thread.currentThread().getName());
  }
  
  @Override
  public long getEntries() {
    if (isEnabled()) {
      long result = 0;
      for (Tablet tablet : server.getOnlineTablets()) {
        result += tablet.getNumEntries();
      }
      return result;
    }
    return 0;
  }

  @Override
  public long getEntriesInMemory() {
    if (isEnabled()) {
      long result = 0;
      for (Tablet tablet : server.getOnlineTablets()) {
        result += tablet.getNumEntriesInMemory();
      }
      return result;
    }
    return 0;
  }

  @Override
  public long getIngest() {
    if (isEnabled()) {
      long result = 0;
      for (Tablet tablet : server.getOnlineTablets()) {
        result += tablet.getNumEntriesInMemory();
      }
      return result;
    }
    return 0;
  }

  @Override
  public int getMajorCompactions() {
    if (isEnabled()) {
      int result = 0;
      for (Tablet tablet : server.getOnlineTablets()) {
        if (tablet.isMajorCompactionRunning())
          result++;
      }
      return result;
    }
    return 0;
  }

  @Override
  public int getMajorCompactionsQueued() {
    if (isEnabled()) {
      int result = 0;
      for (Tablet tablet : server.getOnlineTablets()) {
        if (tablet.isMajorCompactionQueued())
          result++;
      }
      return result;
    }
    return 0;
  }

  @Override
  public int getMinorCompactions() {
    if (isEnabled()) {
      int result = 0;
      for (Tablet tablet : server.getOnlineTablets()) {
        if (tablet.isMinorCompactionRunning())
          result++;
      }
      return result;
    }
    return 0;
  }

  @Override
  public int getMinorCompactionsQueued() {
    if (isEnabled()) {
      int result = 0;
      for (Tablet tablet : server.getOnlineTablets()) {
        if (tablet.isMinorCompactionQueued())
          result++;
      }
      return result;
    }
    return 0;
  }

  @Override
  public int getOnlineCount() {
    if (isEnabled())
      return server.getOnlineTablets().size();
    return 0;
  }

  @Override
  public int getOpeningCount() {
    if (isEnabled())
      return server.getOpeningCount();
    return 0;
  }

  @Override
  public long getQueries() {
    if (isEnabled()) {
      long result = 0;
      for (Tablet tablet : server.getOnlineTablets()) {
        result += tablet.totalQueries();
      }
      return result;
    }
    return 0;
  }

  @Override
  public int getUnopenedCount() {
    if (isEnabled())
      return server.getUnopenedCount();
    return 0;
  }

  @Override
  public String getName() {
    if (isEnabled())
      return server.getClientAddressString();
    return "";
  }

  @Override
  public long getTotalMinorCompactions() {
    if (isEnabled())
      return server.getTotalMinorCompactions();
    return 0;
  }

  @Override
  public double getHoldTime() {
    if (isEnabled())
      return server.getHoldTimeMillis() / 1000.;
    return 0;
  }

  @Override
  public double getAverageFilesPerTablet() {
    if (isEnabled()) {
      int count = 0;
      long result = 0;
      for (Tablet tablet : server.getOnlineTablets()) {
        result += tablet.getDatafiles().size();
        count++;
      }
      if (count == 0)
        return 0;
      return result / (double) count;
    }
    return 0;
  }

  @Override
  protected ObjectName getObjectName() {
    return OBJECT_NAME;
  }

  @Override
  protected String getMetricsPrefix() {
    return METRICS_PREFIX;
  }

}
