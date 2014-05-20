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
package org.apache.accumulo.tserver.replication;

import java.util.concurrent.ThreadPoolExecutor;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.server.zookeeper.DistributedWorkQueue;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class ReplicationWorker implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(ReplicationWorker.class);

  private Instance inst;
  private VolumeManager fs;
  private AccumuloConfiguration conf;
  private ThreadPoolExecutor executor;

  public ReplicationWorker(Instance inst, VolumeManager fs, AccumuloConfiguration conf) {
    this.inst = inst;
    this.fs = fs;
    this.conf = conf;
  }

  public void setExecutor(ThreadPoolExecutor executor) {
    this.executor = executor;
  }

  @Override
  public void run() {
    DefaultConfiguration defaultConf = DefaultConfiguration.getDefaultConfiguration();
    long defaultDelay = defaultConf.getTimeInMillis(Property.REPLICATION_WORK_PROCESSOR_DELAY);
    long defaultPeriod = defaultConf.getTimeInMillis(Property.REPLICATION_WORK_PROCESSOR_PERIOD);
    long delay = conf.getTimeInMillis(Property.REPLICATION_WORK_PROCESSOR_DELAY);
    long period = conf.getTimeInMillis(Property.REPLICATION_WORK_PROCESSOR_PERIOD);
    try {
      DistributedWorkQueue workQueue;
      if (defaultDelay != delay && defaultPeriod != period) {
        log.debug("Configuration DistributedWorkQueue with delay and period of {} and {}", delay, period);
        workQueue = new DistributedWorkQueue(ZooUtil.getRoot(inst) + Constants.ZREPLICATION_WORK_QUEUE, conf, delay, period);
      } else {
        log.debug("Configuring DistributedWorkQueue with default delay and period");
        workQueue = new DistributedWorkQueue(ZooUtil.getRoot(inst) + Constants.ZREPLICATION_WORK_QUEUE, conf);
      }

      workQueue.startProcessing(new ReplicationProcessor(inst, conf, fs, SystemCredentials.get()), executor);
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
