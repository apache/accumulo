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
import javax.management.StandardMBean;

import org.apache.accumulo.server.metrics.AbstractMetricsImpl;
import org.apache.accumulo.tserver.TabletServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TabletServerMBeanImpl extends AbstractMetricsImpl implements TabletServerMBean {
  private static final Logger log = LoggerFactory.getLogger(TabletServerMBeanImpl.class);
  private static final String METRICS_PREFIX = "tserver";
  private ObjectName OBJECT_NAME = null;

  private final TabletServerMetricsUtil util;

  TabletServerMBeanImpl(TabletServer server) {
    util = new TabletServerMetricsUtil(server);
    try {
      OBJECT_NAME = new ObjectName("accumulo.server.metrics:service=TServerInfo,name=TabletServerMBean,instance=" + Thread.currentThread().getName());
    } catch (MalformedObjectNameException e) {
      log.error("Exception setting MBean object name", e);
    }
  }

  @Override
  public void register() throws Exception {
    // Do this because interface not in same package.
    StandardMBean mbean = new StandardMBean(this, TabletServerMBean.class, false);
    register(mbean);
  }

  @Override
  public long getEntries() {
    if (isEnabled()) {
      return util.getEntries();
    }
    return 0;
  }

  @Override
  public long getEntriesInMemory() {
    if (isEnabled()) {
      return util.getEntriesInMemory();
    }
    return 0;
  }

  @Override
  public double getIngest() {
    if (isEnabled()) {
      return util.getIngest();
    }
    return 0;
  }

  @Override
  public int getMajorCompactions() {
    if (isEnabled()) {
      return util.getMajorCompactions();
    }
    return 0;
  }

  @Override
  public int getMajorCompactionsQueued() {
    if (isEnabled()) {
      return util.getMajorCompactionsQueued();
    }
    return 0;
  }

  @Override
  public int getMinorCompactions() {
    if (isEnabled()) {
      return util.getMinorCompactions();
    }
    return 0;
  }

  @Override
  public int getMinorCompactionsQueued() {
    if (isEnabled()) {
      return util.getMinorCompactionsQueued();
    }
    return 0;
  }

  @Override
  public int getOnlineCount() {
    if (isEnabled())
      return util.getOnlineCount();
    return 0;
  }

  @Override
  public int getOpeningCount() {
    if (isEnabled())
      return util.getOpeningCount();
    return 0;
  }

  @Override
  public long getQueries() {
    if (isEnabled()) {
      return util.getQueries();
    }
    return 0;
  }

  @Override
  public int getUnopenedCount() {
    if (isEnabled())
      return util.getUnopenedCount();
    return 0;
  }

  @Override
  public String getName() {
    if (isEnabled())
      return util.getName();
    return "";
  }

  @Override
  public long getTotalMinorCompactions() {
    if (isEnabled())
      return util.getTotalMinorCompactions();
    return 0;
  }

  @Override
  public double getHoldTime() {
    if (isEnabled())
      return util.getHoldTime();
    return 0;
  }

  @Override
  public double getAverageFilesPerTablet() {
    if (isEnabled()) {
      return util.getAverageFilesPerTablet();
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
